import json
import threading

import time

import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

class PingServer(threading.Thread):
    def __init__(self, server_who_ping, server_list, redis_client):
        threading.Thread.__init__(self)
        self.server_who_ping = server_who_ping
        self.server_list = server_list
        self.redis_client = redis_client
        self.pubsub = self.redis_client.pubsub()
        self.channel_name = "cluster_management_channel"
        self.pubsub.subscribe(self.channel_name)

    def run(self):

        logging.info("Start ping servers")
        alive_servers = dict()
        n = 0
        reponse = None

        # Je fais l'appel
        self.redis_client.publish(
            self.channel_name,
            json.dumps({"question": "who_is_alive"})
        )

        # Stop listening after n seconds
        while n < 10:
            message = self.pubsub.get_message()
            if message :
                if message['data'] == 1 :
                    pass
                else :
                    response = json.loads(message['data'].decode('utf-8'))
                    logging.info("receive")
                    logging.info(response)

                    if not "question" in response.keys():
                        if response['id'] != self.server_who_ping:
                            alive_servers[response['ordinal']] = response
            n = n + 1
            time.sleep(1)
            time.sleep(1)
        # Je remplace la vielle liste de serveurs par une copie la nouvelle
        logging.info("Stop waiting")
        self.server_list = alive_servers.copy()
