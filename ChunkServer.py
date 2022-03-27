from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import os
import pickle
import socket
import sys
import threading
import time

import py_eureka_client.eureka_client as eureka_client

class ChunkServer():
    def __init__(self, host, port):
        eureka_client.init(eureka_server="localhost:8761", app_name=("chunk_server_" + host), instance_port= port)
        res = eureka_client.get_application(eureka_server="http://localhost:8761/eureka/", app_name="MASTER")
        print("result: " + str(res.get_instance("1").ipAddr) + " port: " + str(res.get_instance("1").port.port))
        self.master_proxy = ServerProxy('http://' + "localhost" + ":" + str(res.get_instance("1").port.port))
        self.root_dir = '/home/mohamed/temp/' + str(port) + '/'
        self.url = 'http://' + host + ':' + str(port)
        self.checksum_size_bytes = 64
        self.chunk_id_to_filename = {} # int chunk_id -> str filename
        self.chunk_id_to_version = {} # int chunk_id -> int version
        self.chunk_id_to_new_data = {} # int chunk_id -> list[str] new data written
        self.recently_applied_data = {} # data -> int offset
        # (int chunk_id, int offset in chunk) -> int checksum of data
        self.chunk_idx_to_checksum = {}
        # to avoid deadlock with xmlrpc -
        # interrupt and retry when deadlock is detected
        socket.setdefaulttimeout(2)

        self.init_from_files()
        chunk_list = self.get_chunks()
        stale_chunks = self.master_proxy.link_with_master(self.url, chunk_list)
        if len(stale_chunks) > 0:
            print('removing stale chunks')
            self.remove_chunks(stale_chunks)

        self.thread_interval = 30
        background_thread = threading.Thread(target=self.background_thread, args=())
        background_thread.daemon = True
        background_thread.start()
        print('server initialized and linked with master')

    # init_from_files:
    # Initializes the chunkserver from data it has persistently written to disk (if any).
    def init_from_files(self):
        # initialize self.chunk_idx_to_checksum from disk
        if os.path.isfile(self.root_dir + 'chunk_checksums.pickle'):
            with open(self.root_dir + 'chunk_checksums.pickle', 'rb') as f:
                self.chunk_idx_to_checksum = pickle.load(f)

        # initialize self.chunk_id_to_version from disk
        if os.path.isfile(self.root_dir + 'chunk_versions.pickle'):
            with open(self.root_dir + 'chunk_versions.pickle', 'rb') as f:
                self.chunk_id_to_version = pickle.load(f)

        # initialize self.chunk_id_to_filename based on files existing on disk
        chunk_files = os.listdir(self.root_dir)
        for filename in chunk_files:
            if filename == '.DS_Store' or filename == 'chunk_versions.pickle' \
                    or filename == 'chunk_checksums.pickle':
                continue
            underscore_idx = filename.rfind('_')
            chunk_id = int(filename[underscore_idx+1:])
            self.chunk_id_to_filename[chunk_id] = filename
        print('initialized from files:', self.chunk_id_to_filename)

    # remove_chunks:
    # Remove the provided chunks from the chunkserver's metadata and delete the corresponding
    # file from storage.
    def remove_chunks(self, deleted_chunk_ids):
        for chunk_id in deleted_chunk_ids:
            filename = self.chunk_id_to_filename[chunk_id]
            os.remove(self.root_dir + filename)
            del self.chunk_id_to_filename[chunk_id]
            del self.chunk_id_to_version[chunk_id]
            tups_to_delete = []
            for (c_id, c_idx) in self.chunk_idx_to_checksum:
                if c_id == chunk_id:
                    tups_to_delete.append((c_id, c_idx))
            for tup in tups_to_delete:
                del self.chunk_idx_to_checksum[tup]
        if len(deleted_chunk_ids) > 0:
            print('deleted chunk ids:', deleted_chunk_ids)
            with open(self.root_dir + 'chunk_versions.pickle', 'wb') as f:
                pickle.dump(self.chunk_id_to_version, f)
            with open(self.root_dir + 'chunk_checksums.pickle', 'wb') as f:
                pickle.dump(self.chunk_idx_to_checksum, f)
    
    # background_thread:
    # Thread that continuously runs to handle heartbeats to the master.
    def background_thread(self):
        while True:
            time.sleep(self.thread_interval)
            chunk_ids = self.get_chunks()
            while True:
                try:
                    deleted_chunk_ids = self.master_proxy.heartbeat(self.url, chunk_ids)
                    break
                except:
                    print('error connecting to master, retrying in 10 seconds...')
                    time.sleep(10)
            self.remove_chunks(deleted_chunk_ids)

    # replicate_data:
    # Called by the master in order to tell this chunkserver to copy data from another
    # chunkserver.
    def replicate_data(self, chunk_id, version, replica_url):
        replica_proxy = ServerProxy(replica_url)
        res = replica_proxy.get_data_for_chunk(chunk_id, version)
        if res == 'stale replica' or res == 'invalid checksum':
            return res
        filename = res[0]
        data = res[1]
        version = res[2]
        with open(self.root_dir + filename, 'w') as f:
            f.write(data)
        self.update_version(chunk_id, version)
        self.chunk_id_to_filename[chunk_id] = filename

        # write checksum
        for i in range(0, len(data), self.checksum_size_bytes):
            cksm_idx = i // self.checksum_size_bytes
            self.chunk_idx_to_checksum[(chunk_id, cksm_idx)] = 0
            self.update_checksum((chunk_id, cksm_idx), data[i:i+self.checksum_size_bytes])
        with open(self.root_dir + 'chunk_checksums.pickle', 'wb') as f:
            pickle.dump(self.chunk_idx_to_checksum, f)
        print('copied chunk', chunk_id, 'from', replica_url, 'to myself')
        return 'success'

    # get_data_for_chunk:
    # Called by another chunkserver when it is requesting to copy data from this chunkserver.
    def get_data_for_chunk(self, chunk_id, version):
        if version > self.chunk_id_to_version[chunk_id]:
            return 'stale replica'
        filename = self.chunk_id_to_filename[chunk_id]
        res = None
        with open(self.root_dir + filename) as f:
            res = f.read()
            valid_checksum = self.validate_checksum(res, chunk_id, 0)
            if not valid_checksum:
                print('reporting invalid checksum to master')
                self.master_proxy.report_invalid_checksum(chunk_id, self.url)
                # delete own copy of chunk
                print('deleting own copy of chunk')
                self.remove_chunks([chunk_id])
                return 'invalid checksum'
        version = self.chunk_id_to_version[chunk_id]
        print('sending data for', filename, chunk_id, 'to other replica')
        return (filename, res, version)

    # get_chunks:
    # Helper function to return all chunks on this chunkserver.
    def get_chunks(self):
        print('sending list of owned chunks to master')
        cids = list(self.chunk_id_to_filename.keys())
        chunks = [(cid, self.chunk_id_to_version[cid]) for cid in cids]
        return chunks

    # update_version:
    # Called by the client for each chunkserver in order to update the version
    # of the specified chunk.
    def update_version(self, chunk_id, new_version): #needs to be called in Write when writing in parallel
        if chunk_id in self.chunk_id_to_version:
            cur_version = self.chunk_id_to_version[chunk_id]
            print('updating version of', chunk_id, 'from', cur_version, 'to', new_version)
        else:
            print('creating new version', new_version, 'for chunk', chunk_id)
        self.chunk_id_to_version[chunk_id] = new_version
        with open(self.root_dir + 'chunk_versions.pickle', 'wb') as f:
            pickle.dump(self.chunk_id_to_version, f)

    # create:
    # Called by the master to create a file on this server.
    def create(self, filename, chunk_id):
        chunk_filename = filename + '_' + str(chunk_id)
        with open(self.root_dir + chunk_filename, 'w') as f:
            pass
        self.chunk_idx_to_checksum[(chunk_id, 0)] = 0
        with open(self.root_dir + 'chunk_checksums.pickle', 'wb') as f:
            pickle.dump(self.chunk_idx_to_checksum, f)
        self.chunk_id_to_filename[chunk_id] = chunk_filename
        self.update_version(chunk_id, 0)
        print('chunk_id_to_filename:', self.chunk_id_to_filename)

    # validate_checksum:
    # Validates the checksum of the provided data.
    def validate_checksum(self, data, chunk_id, cksm_offset):
        for i in range(cksm_offset, cksm_offset + len(data), self.checksum_size_bytes):
            cksm_idx = i // self.checksum_size_bytes
            if (chunk_id, cksm_idx) not in self.chunk_idx_to_checksum:
                print('did not find checksum for chunk', chunk_id, cksm_idx)
                return False
            stored_checksum = self.chunk_idx_to_checksum[(chunk_id, cksm_idx)]
            data_slice = data[i-cksm_offset:i-cksm_offset+self.checksum_size_bytes]
            data_checksum = 0
            for l in data_slice:
                data_checksum += ord(l)
            if data_checksum != stored_checksum:
                print('checksum did not match for chunk', chunk_id, cksm_idx)
                return False
        return True

    # read:
    # Called by the client to read from a file.
    def read(self, chunk_id, chunk_offset, amount):
        print('reading chunk', chunk_id, 'for', amount, 'bytes')
        filename = self.root_dir + self.chunk_id_to_filename[chunk_id]
        with open(filename) as f:
            # find nearest checksum offset of given chunk_offset
            cksm_offset = chunk_offset - (chunk_offset % self.checksum_size_bytes)
            # round amount up to nearest multiple of checksum size
            remainder = amount % self.checksum_size_bytes
            if remainder == 0:
                cksm_amount = amount
            else:
                cksm_amount = amount + self.checksum_size_bytes - remainder
            f.seek(cksm_offset)
            res = f.read(cksm_amount)
            valid_checksum = self.validate_checksum(res, chunk_id, cksm_offset)
            if not valid_checksum:
                print('reporting invalid checksum to master')
                self.master_proxy.report_invalid_checksum(chunk_id, self.url)
                # delete own copy of chunk
                print('deleting own copy of chunk')
                self.remove_chunks([chunk_id])
                return None
            diff = chunk_offset - cksm_offset
            return res[diff:diff + amount]


    # send_data:
    # Called by the client to send data to chunkservers.
    def send_data(self, chunk_id, data):
        if chunk_id not in self.chunk_id_to_new_data:
            self.chunk_id_to_new_data[chunk_id] = []
        self.chunk_id_to_new_data[chunk_id].append(data)
        print('done storing')
        return 'success'

    # update_checksum:
    # Updates the checksum given new data.
    def update_checksum(self, chunk_id_and_idx, data):
        new_checksum = 0
        for d in data:
            new_checksum += ord(d)
        self.chunk_idx_to_checksum[chunk_id_and_idx] += new_checksum

    # apply_mutations:
    # Called by the client to tell the replica to apply mutations
    def apply_mutations(self, chunk_id, new_mutations, data_to_offset):
        if chunk_id not in self.chunk_id_to_new_data:
            # no new data, must have been applied already.
            # return recently applied data offsets instead
            return self.recently_applied_data
        new_mutations = self.chunk_id_to_new_data[chunk_id][:]

        del self.chunk_id_to_new_data[chunk_id]
        filename = self.chunk_id_to_filename[chunk_id]
        with open(self.root_dir + filename, 'a') as f:
            for data in new_mutations:
                # write data
                curr_offset = f.tell()
                f.write(data)
                data_to_offset[data] = curr_offset

                # compute checksum
                data_ptr = 0
                cksm_idx = curr_offset // self.checksum_size_bytes
                cksm_offset = curr_offset % self.checksum_size_bytes
                while data_ptr < len(data):
                    data_slice_len = min(self.checksum_size_bytes - cksm_offset, \
                            len(data) - data_ptr)
                    data_slice = data[data_ptr:data_ptr + data_slice_len]
                    if (chunk_id, cksm_idx) not in self.chunk_idx_to_checksum:
                        self.chunk_idx_to_checksum[(chunk_id, cksm_idx)] = 0
                    self.update_checksum((chunk_id, cksm_idx), data_slice)
                    data_ptr += data_slice_len
                    cksm_idx += 1
                    cksm_offset = 0
        with open(self.root_dir + 'chunk_checksums.pickle', 'wb') as f:
            pickle.dump(self.chunk_idx_to_checksum, f)
        return data_to_offset

def main():
    host = sys.argv[1]
    port = sys.argv[2]
    server = SimpleXMLRPCServer((host, int(port)), allow_none=True)
    server.register_introspection_functions()
    server.register_instance(ChunkServer(host,int(port)))
    server.serve_forever()

if __name__ == '__main__':
    main()