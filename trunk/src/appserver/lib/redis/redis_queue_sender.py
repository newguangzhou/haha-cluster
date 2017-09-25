# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger(__name__)
import threading
from redis_queue import RedisQueue
from tornado.ioloop import IOLoop
import time
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor


class RedisQueueSender(object):
    executor = ThreadPoolExecutor(2)

    def __init__(self, redis_mgr):
        self.redis_mgr = redis_mgr
        self.queue = RedisQueue(redis_mgr)

    @run_on_executor
    def send(self, service_name, data):
        return self.queue.send_msg(service_name, data)
