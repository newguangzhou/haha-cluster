# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger(__name__)
import time
import threading
from tornado.ioloop import IOLoop
class RedisTimer(object):
    def __init__(self, redis_mgr, on_keys_expire_func, name, check_num=10):
        self.redis_mgr = redis_mgr
        self.name = name
        self.check_num = check_num
        self.on_keys_expire_func = on_keys_expire_func
        self.stared = False
    def add_key(self, key, expires):
        now = int(time.time())
        timeout = now + expires
        client = self.redis_mgr.get_client()
        client.zadd(self.name, timeout, key)
       
        logger.debug("add key:%s timeout:%d", key, timeout)

    def start(self):
        threading.Thread(target=self._real_start).start()
        self.stared = True

    def is_started(self):
        return self.stared
             
    def _on_keys_expire(self, expire_keys):
        logger.info("name:%s, expire_keys:%s", self.name, expire_keys)
        self.on_keys_expire_func(expire_keys)
        
    def _real_start(self):
        while True:
            expire_keys = self.get_expire_keys(self.check_num)
            logger.debug("check name:%s expire_keys:%s", self.name, str(expire_keys))
            if len(expire_keys) != 0:
                IOLoop.instance().add_callback(self._on_keys_expire, expire_keys)
            time.sleep(1)     
            

    def get_expire_keys(self, num):
        client = self.redis_mgr.get_client()
        key_with_scores = client.zrange(self.name, 0, num, withscores=True)
        now = int(time.time())
        expire_keys = []
        for key, score in key_with_scores:
            if score <= now:
                expire_keys.append(key)
                client.zrem(self.name,key)
                logger.debug("zrem name:%s key:%s time:%d now:%d",self.name, key, score, now)
            else:
                break
        return expire_keys
