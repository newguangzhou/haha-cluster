# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger(__name__)

class RedisQueueException(Exception):
    def __init__(self, message, *args):
        self._msg = message % tuple(args)

    def __str__(self):
        return self._msg


class RedisQueue(object):
    def __init__(self, redis_mgr):
        self.redis_mgr = redis_mgr
    
    def subscribe(self, service_name, key):
        client = self.redis_mgr.get_client()
        return client.sadd(service_name, key)

    def un_subscribe(self, service_name, key):
        client = self.redis_mgr.get_client()
        return client.srem(service_name, key)

    def send_msg(self, service_name, item):
        client = self.redis_mgr.get_client()
        keys = client.smembers(service_name)
        if keys and len(keys) >0:
            for key in keys:
                try:
                    client.rpush(key, item)
                    logger.info("send service_name:%s key:%s item:%s success", service_name, key, item)
                except Exception, e:
                    logger.exception("send service_name:%s key:%s item:%s error", service_name, key, item)      
        else:
            logger.warn(" no key in server_name:%s", service_name)
        return 

    def qsize(self, key):
        client = self.redis_mgr.get_client()
        return client.llen()

    def empty(self, key):
        client = self.redis_mgr.get_client()
        return self.qsize(key) == 0

    def put(self, key, item):
        client = self.redis_mgr.get_client()
        return client.rpush(key, item)

    def get(self, key, block=True, timeout=None):
        client = self.redis_mgr.get_client()
        if block:
            item = client.blpop(key, timeout=timeout)
        else:
            item = client.lpop(key)
        if item:
            item = item[1]
        return item

    def get_msg(self, key, entity, block=True, timeout=None):
        item = self.get(key, block, timeout)
        entity.from_json_str(item)
        return 

    def get_nowait(self, key):
        return self.get(key,False)