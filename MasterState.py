from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import os.path
import pickle
import random
import threading
import time
import sys

import py_eureka_client.eureka_client as eureka_client

class MasterState():
    def __init__(self):
        random.seed(0)

        self.root_dir = '/home/mohamed/temp/master/'
        self.thread_interval = 30

        your_rest_server_port = 9001
        self._eureka_client = eureka_client.init(eureka_server="http://localhost:8761",
                                app_name="MasterState",
                                instance_port=your_rest_server_port)
        print('masterState registered')

        background_thread = threading.Thread(target=self.background_thread, \
                args=[self.thread_interval])
        background_thread.daemon = True
        background_thread.start()
        print('master initialized')


    # flush_to_log:
    # Helper function to flush necessary in-memory data to disk.
    def flush_to_state_log(self, data):
        with open(self.root_dir + 'log.txt', 'wb') as f:
            pickle.dump(data, f)
        print('flushed metadata to log')

    def background_thread(self, interval):
        while True:
            if(self._eureka_client.applications.get_application("Master") == None):
                exec(open("Master.py").read())
                exit()
            time.sleep(interval)

#Main
def main():
    master_server = SimpleXMLRPCServer(('localhost', 9001), allow_none=True)
    master_server.register_introspection_functions()
    master_server.register_instance(MasterState())
    master_server.serve_forever()

if __name__ == '__main__':
    main()