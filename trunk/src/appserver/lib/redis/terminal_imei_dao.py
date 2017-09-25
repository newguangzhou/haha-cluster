# -*- coding: utf-8 -*-
from redis_mgr import RedisMgr
import logging
logger = logging.getLogger(__name__)
from tornado.ioloop import IOLoop
TERMINAL_IMEI_PREFIX = "terminal_imei"
TERMINAL_IMEI_TAG = "ti"

REMOVE_SUCCESS = 0 
REMOVE_NOT_FOUND = 1
REMOVE_NOT_EQUAL = 2
class TerminalImeiDao(object):
    def __init__(self, redis_mgr):
        self.redis_mgr = redis_mgr

    def get_key(self, imei):
        return  "{%s}%s:%s" %(TERMINAL_IMEI_TAG, TERMINAL_IMEI_PREFIX, imei)

    def add(self, imei, server_id):
        client = self.redis_mgr.get_client()
        return client.set(self.get_key(imei), server_id)

    def get_server_id(self, imei):
        client = self.redis_mgr.get_client()
        return client.get(self.get_key(imei))


    #todo 不是原子操作
    def remove(self, imei, cur_server_id):
        ret = REMOVE_SUCCESS
       
        server_id = self.get_server_id(imei)
        if not server_id:
            ret = REMOVE_NOT_FOUND 
            return  ret
        if int(server_id) != cur_server_id:
            ret = REMOVE_NOT_EQUAL
            return ret
        client = self.redis_mgr.get_client()
        client.delete(self.get_key(imei))
        return    ret 
            
    
    