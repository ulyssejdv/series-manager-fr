
import redis
import threading
import logging
import time
import json

from threading import Timer

from clustering.redisimpl.clusterping import PingServer

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

class ClusterAvailabilityCheck(threading.Thread):

    def __init__(self, redis, server_id, url, _queue_, presence_interval):

        threading.Thread.__init__(self)

        self.redis = redis
        self.server_id = server_id
        self.channel_name = "cluster_management_channel"
        self._queue_ = _queue_
        self.presence_interval = presence_interval
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(self.channel_name)
        self.bootstrap = True
        self.servers = dict()
        self.ordinal = -1
        self.cluster_availability = None
        self.server_url = url

        self.timer = Timer(2*self.presence_interval, self.end_of_bootstrap )
        self.timer.start()


    def end_of_bootstrap(self):
        self.bootstrap = False
        max_ordinal = -1
        for ordinal in self.servers.keys() :
            if ordinal > max_ordinal :
                max_ordinal = ordinal

        if -1 == max_ordinal :
            self.ordinal = 0
            logging.info("%s is master", self.server_id)

        else :
            #logging.info("max_ordinal = %s", max_ordinal)
            self.ordinal = 1 + max_ordinal
            logging.info("%s is backup", self.server_id)

        if self.cluster_availability :
            self.cluster_availability.set_ordinal(self.ordinal)
            self.cluster_availability.publishClusterPresence()


    def set_cluster_availability(self, cluster_availability):
        self.cluster_availability = cluster_availability


    def is_master(self):
        if self.bootstrap :
            return False

        for ordinal in self.servers.keys():
            if self.ordinal > ordinal :
                return False

        return True


    def get_instance_urls(self):
        urls = list()
        for ordinal in self.servers.keys():
            status = self.servers[ordinal]
            logging.info("%s", status)
            urls.append(status['url'])
        return urls


    def get_master_url(self):
        _ordinal_ = -1
        for ordinal in self.servers.keys():
            if ordinal == self.ordinal :
                continue

            if _ordinal_ == -1 :
                _ordinal_ = ordinal
            else :
                if ordinal < _ordinal_ :
                    _ordinal_ = ordinal

        status = self.servers[_ordinal_]
        return status['url'];


    def run(self):

        #logging.info("Cluster availability check thread routine running.")
        n = 0
        while True :
            message = self.pubsub.get_message()
            if message :
                # logging.info("RECEIVED = %s", message)
                if message['data'] == 1 :
                    None
                else :
                    status = json.loads(message['data'].decode('utf-8'))

                    if "question" in status.keys():
                        if status["question"] == "who_is_alive":
                            logging.info("Some One Ping Me")
                            self.cluster_availability.publishClusterPresence()
                    else:
                        if self.server_id == status['id'] :
                            #logging.info("From Myself = %s", status['id'])
                            None
                        else:
                            logging.info("Server ID = %s Ordinal = %d on cluster", status['id'], status['ordinal'])
                            self.servers[status['ordinal']] = status

            if n > 30:
                n = 0
                PingServer(self.server_id, self.servers, self.redis).start()
                logging.info("Server list updated")
                logging.info(self.servers)


            n = n + 1
            time.sleep(1)