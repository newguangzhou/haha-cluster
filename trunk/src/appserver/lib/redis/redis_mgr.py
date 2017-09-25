# -*- coding: utf-8 -*-
import redis
from rediscluster import StrictRedisCluster


class RedisMgr(object):
    def __init__(self, startup_nodes, max_connections=100):
        #connection_pool = redis.ConnectionPool()
        self.redis_client = StrictRedisCluster(max_connections=max_connections,
                                               startup_nodes=startup_nodes,
                                               socket_timeout=5)


    def get_client(self):
        return self.redis_client


def main():
    pass
    #redis_mgr = RedisMgr(startup_nodes)

if __name__ == '__main__':
    main()