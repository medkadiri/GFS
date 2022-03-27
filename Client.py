from xmlrpc.client import ServerProxy

import random
import time

import py_eureka_client.eureka_client as eureka_client

class Client():
    def __init__(self, chunk_size=64000, cache_timeout=60, debug=False):
        random.seed(0)
        self.debug = debug
        self.chunk_size = chunk_size
        eureka_client.init(eureka_server="localhost:8761", app_name="client", instance_port= 9999)
        res = eureka_client.get_application(eureka_server="http://localhost:8761/eureka/", app_name="MASTER")
        print("result: " + str(res.get_instance("1").ipAddr))
        self.master_proxy = ServerProxy('http://' + str(res.get_instance("1").ipAddr) + ':9000')
        # read_cache: (filename,chunk_idx) -> [chunk_id,[replica urls],time]
        # used for caching the replica urls of a given file and chunk index when reading
        self.read_cache = {}
        self.read_cache_timeout_secs = cache_timeout
        # replicas_cache: (filename,chunk_idx) -> [chunk_id,primary,[replica urls]]
        # used for caching the primary of a given file and chunk index when writing
        self.replicas_cache = {}

    # create:
    # Tell the master to create a file in GFS with the given filename.
    def create(self, filename):
        return self.master_proxy.create(filename)

    # delete:
    # Delete cached entries for filename, then tell the master to delete the file
    # with the given filename from GFS.
    def delete(self, filename):
        to_delete = []
        for f, chunk_idx in self.replicas_cache:
            if f == filename:
                to_delete.append((f, chunk_idx))
        for tup in to_delete:
            del self.replicas_cache[tup]
        return self.master_proxy.delete(filename)

    # read:
    # Read amount bytes from thespecified filename, starting at a given offset.
    # Splits the amount into different chunks if the read overlaps chunks.
    def read(self, filename, byte_offset, amount):
        chunk_idx = byte_offset // self.chunk_size
        chunk_offset = byte_offset % self.chunk_size

        total_to_read = amount
        amount_to_read_in_chunk = min(self.chunk_size - chunk_offset, total_to_read)
        s = ''
        while total_to_read > 0:
            # locate replica urls of chunk
            should_contact_master = True
            if (filename, chunk_idx) in self.read_cache:
                res = self.read_cache[(filename, chunk_idx)]
                original_cache_time = res[2]
                if time.time() <= original_cache_time + self.read_cache_timeout_secs:
                    #refresh original cache time
                    self.read_cache[(filename, chunk_idx)][2] = time.time()
                    should_contact_master = False

            if should_contact_master:
                res = self.master_proxy.read(filename, chunk_idx)
                if res == 'file not found' or res == 'requested chunk idx out of range':
                    return res
                res.append(time.time())
                self.read_cache[(filename,chunk_idx)] = res
            chunk_id = res[0]
            replica_urls = res[1]

            # pick random replica to read from
            while len(replica_urls) > 0:
                replica_url = random.choice(replica_urls)
                chunkserver_proxy = ServerProxy(replica_url)
                try:
                    read_res = chunkserver_proxy.read( \
                            chunk_id, chunk_offset, amount_to_read_in_chunk)
                    if read_res == None:
                        if self.debug:
                            print('invalid checksum found in replica, trying another')
                        replica_urls.remove(replica_url)
                        continue
                    s += read_res
                    break
                except Exception as e:
                    if self.debug:
                        print('exception reading from replica:', e)
                    replica_urls.remove(replica_url)
            if len(replica_urls) == 0:
                return 'no replicas remaining for chunk'

            total_to_read -= amount_to_read_in_chunk
            amount_to_read_in_chunk = min(self.chunk_size, total_to_read)
            chunk_idx += 1
            chunk_offset = 0
        return s

    # write:
    # Writes the given data to the desired file. The client specifies the byte_offset,
    # which must be the last byte written so far in the file.
    # Splits writes that overlap chunks into multiple writes.
    def write(self, filename, data, byte_offset):
        chunk_idx = byte_offset // self.chunk_size
        first_chunk_idx = chunk_idx
        chunk_offset = byte_offset % self.chunk_size
        
        data_idx = 0
        total_to_write = len(data)
        amount_to_write_in_chunk = min(self.chunk_size - chunk_offset, total_to_write)

        backoff_secs = 1
        offset_written_at = None
        while total_to_write > 0:
            if chunk_idx != first_chunk_idx:
                # if this isn't the first chunk, create another chunk
                self.create(filename)
            data_piece = data[data_idx:data_idx+amount_to_write_in_chunk]
            res = self.write_helper(filename, data_piece, chunk_idx)
            if type(res) != int:
                if res == 'file not found':
                    return res
                # error, wait for exponential backoff and retry
                print(res, 'for ' + filename + ', backing off and retrying in ' \
                        + str(backoff_secs) + ' seconds')
                time.sleep(backoff_secs)
                backoff_secs *= 2
            else:
                if chunk_idx == first_chunk_idx:
                    offset_written_at = res
                chunk_idx += 1
                data_idx += amount_to_write_in_chunk
                total_to_write -= amount_to_write_in_chunk
                amount_to_write_in_chunk = min(self.chunk_size, total_to_write)
        return offset_written_at

    # write_helper:
    # Helper method to handle sending data to replicas as well as actually applying the
    # mutations to those replicas.
    def write_helper(self, filename, data, chunk_idx):
        if (filename, chunk_idx) in self.replicas_cache:
            res = self.replicas_cache[(filename,chunk_idx)]
            if self.debug:
                print('using from replicas cache:', res)
        else:
            res = self.master_proxy.get_replicas(filename, chunk_idx)
            if res == 'file not found':
                return res
            self.replicas_cache[(filename,chunk_idx)] = res
            if self.debug:
                print('asked master for replicas:', res)
        chunk_id = res[0]
        replica_urls = res[1]

        # send data to all replicas
        i = 0
        while i < len(replica_urls):
            replica_url = replica_urls[i]
            chunkserver_proxy = ServerProxy(replica_url)
            try:
                send_res = chunkserver_proxy.send_data(chunk_id, data)
            except:
                if self.debug:
                    print('exception when sending data to', replica_url)
                send_res = 'chunkserver failure_' + replica_url
        # if any replica failed, reselect replicas
            if send_res != 'success':
                if self.debug:
                    print('replica failed when sending data, asking for replicas again')
                    print('res=', send_res)
                if send_res == 'timed out':
                    time.sleep(2)
                    continue
                failed_url = send_res[send_res.rfind('_')+1:]
                self.master_proxy.remove_chunkserver(failed_url)
                new_replicas_res = self.master_proxy.get_replicas(filename, chunk_idx)
                self.replicas_cache[(filename, chunk_idx)] = new_replicas_res
                chunk_id = new_replicas_res[0]
                replica_urls = new_replicas_res[1]
                i = 0
            else:
                i += 1

        # tell all replicas to apply mutations
        while True:
            # to simplify state machine, assume that primary doesn't fail between selecting
            # it a few lines above and here
            new_mutations = []
            # dictionary mapping data written to offset in file it was written at
            data_to_offset = {}

            for url in replica_urls:
                proxy = ServerProxy(url)
                try:
                    print('Applying mutations to:', url)
                    data_to_offset = proxy.apply_mutations(chunk_id, new_mutations, data_to_offset)
                except:
                    # if chunkserver is down when applying mutations, just skip
                    # and let the master re-replication process take care of it
                    continue

            # find offset of written data in mutation_res
            offset_within_chunk = data_to_offset[data]
            written_offset = (chunk_idx * self.chunk_size) + offset_within_chunk
            return written_offset
