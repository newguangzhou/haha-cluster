# -*- coding: utf-8 -*-

import threading
import logging
import time
import traceback
import random
import copy

from tornado import ioloop, gen
from tornado.concurrent import Future

from concurrent.futures import ThreadPoolExecutor

import pymongo


class MongoDAOBaseException(Exception):
    def __init__(self, message, *args):
        self._msg = message % tuple(args)

    def __str__(self):
        return self._msg


class MongoMeta:
    def __init__(self, *args, **kwargs):
        self.host = "172.19.101.61"
        self.port = 27017
        self.username = None
        self.passwd = None
        self.repl_set_name = None
        self.max_thread_count = 3

        if kwargs.has_key("hosts"):
            self.host = kwargs["hosts"]
        if kwargs.has_key("port"):
            self.port = kwargs["port"]

        if kwargs.has_key("username"):
            self.username = kwargs["username"]

        if kwargs.has_key("passwd"):
            self.passwd = kwargs["passwd"]

        if kwargs.has_key("repl_set_name"):
            self.repl_set_name = kwargs["repl_set_name"]

        if kwargs.has_key("max_thread_count"):
            self.max_thread_count = kwargs["max_thread_count"]


class MongoDAOBase2:
    def __init__(self, meta, **kwargs):
        self._meta = meta
        self._thread_pool = ThreadPoolExecutor(10)
        self._th_local = threading.local()

    def on_thread_init(self, th_local):
        pass

    def get_thread_local(self):
        return self._th_local

    def _inner_callback(self, **kwargs):
        __real_callback = kwargs["__real_callback"]

        if not hasattr(self._th_local, "mongo_client"):
            extra_args = {"w": 1, "j": True}

            logging.debug(
                "host:%s port:%d passwd:%s username:%s extra_args:%s",
                self._meta.host, self._meta.port, self._meta.passwd,
                self._meta.username, str(extra_args))
            mongo_client = pymongo.MongoClient(self._meta.host,
                                               self._meta.port, **extra_args)
            #print mongo_client
            mongo_client.get_database("admin").authenticate(
                self._meta.username,
                self._meta.passwd,
                mechanism='SCRAM-SHA-1')
            self._th_local.mongo_client = mongo_client
            self.on_thread_init(self._th_local)

            logging.info("Init mongo success, host=%s port=%u replset=%s",
                         self._meta.host, self._meta.port,
                         self._meta.repl_set_name)
        kwargs["__mongo_client"] = self._th_local.mongo_client
        ret = __real_callback(self._th_local.mongo_client, **kwargs)
        return ret

    @gen.coroutine
    def submit(self, callback, **kwargs):
        kwargs["__real_callback"] = callback
        ret = yield self._thread_pool.submit(self._inner_callback, **kwargs)
        raise gen.Return(ret)

    def get_meta(self):
        return self._meta





class MongoDAOBase:
    def __init__(self, mongo_client, thread_pool, **kwargs):
        self._mongo_client = mongo_client
        self._thread_pool = thread_pool

    def _inner_callback(self, **kwargs):
        __real_callback = kwargs["__real_callback"]
        ret = __real_callback(self._mongo_client, **kwargs)
        start = time.time()
        logging.info("thread name:%s start:%f ",
                     threading.currentThread().getName(), start)
        return ret

    @gen.coroutine
    def submit(self, callback, **kwargs):
        kwargs["__real_callback"] = callback
        ret = yield self._thread_pool.submit(self._inner_callback, **kwargs)
        raise gen.Return(ret)



def GetMongoClientAndAuth(meta, **kwargs):
    if not kwargs.has_key("w"):
        kwargs["w"] = 1
    if not kwargs.has_key("j"):
        kwargs["j"] = True
    if not kwargs.has_key("maxPoolSize"):
        kwargs["maxPoolSize"] = 200
        
    mongo_client = pymongo.MongoClient(meta.host, meta.port, **kwargs)
    logging.debug("host:%s port:%d passwd:%s username:%s extra_args:%s",
                    meta.host, meta.port, meta.passwd,
                    meta.username, str(kwargs))
    mongo_client.get_database("admin").authenticate(
        meta.username,
        meta.passwd,
        mechanism='SCRAM-SHA-1')
    logging.info("Init mongo success, host=%s port=%u replset=%s",
                    meta.host, meta.port,
                    meta.repl_set_name)
    return mongo_client
    


