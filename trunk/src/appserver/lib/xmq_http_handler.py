# -*- coding: utf-8 -*-
import tornado.web
from tornado import gen
import json

from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
import logging
logger = logging.getLogger(__name__)



class Executor(ThreadPoolExecutor):
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not getattr(cls, "_instance", None):
            cls._instance = ThreadPoolExecutor(5)
        return cls._instance
    
class WithImeiLogHandlerMixin(object):
    def on_log(self, content, imei):
        logger.info("oplog content:%s imei:%s", content, imei)
        op_log_dao = self.settings["op_log_dao"]
        return op_log_dao.add_op_info(imei=unicode(imei), content=unicode(content))    

class BaseHttpHandler(tornado.web.RequestHandler):
    def write_obj(self, res):
        data = json.dumps(res, ensure_ascii=False, encoding='utf8')
        self.write(data)
        
        

class WithBlockFuncHandler(BaseHttpHandler):
    executor = Executor()
    @run_on_executor
    def call_func(self, obj ,func_name, *args, **kargs):
        return getattr(obj, func_name)(*args, **kargs)
    
    