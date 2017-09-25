# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger(__name__)
import threading
from redis_queue import RedisQueue
from tornado.ioloop import IOLoop
import time


class RedisQueueWorker(object):
    def __init__(self, redis_mgr, service_name, custom_key, func_name, callback_to_main_thread=False):
        self.service_name = service_name
        self.func_name = func_name
        self.redis_queue = RedisQueue(redis_mgr)
        self.custom_key = custom_key
        self.redis_queue.subscribe(self.service_name, custom_key)
        self.callback_to_main_thread = callback_to_main_thread
    def _real_start(self):
        while True:
            try:
                item = self.redis_queue.get(self.custom_key)
                if self.callback_to_main_thread:
                    IOLoop.instance().add_callback(self.func_name, item)
                else:
                    self.func_name(item)
            except Exception, e:
                logger.warn("start_work error:%s not found msg", e.message)
                time.sleep(1)

    def start(self):
        threading.Thread(target=self._real_start).start()
