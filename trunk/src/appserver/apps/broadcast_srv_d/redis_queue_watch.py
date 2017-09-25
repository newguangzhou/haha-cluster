# -*- coding: utf-8 -*-
from lib.redis.redis_queue_worker import RedisQueueWorker
from lib.redis.msg_queue_def import *
from lib.redis.terminal_imei_dao import TerminalImeiDao,REMOVE_SUCCESS
from lib import utils, error_codes
from tornado.ioloop import IOLoop
from tornado import gen
import logging
logger = logging.getLogger(__name__)


class RedisQueueWatcher:
    
    def __init__(self, redis_mgr, unreply_msg_mgr, terminal_rpc, op_log_dao):
        self.redis_mgr = redis_mgr
        self.imei_status_key = "broadcast_srv_d_imei_status"
        self.del_unreply_key = "broadcast_srv_d_del_unreply"
        self.first_connect_key = "broadcast_srv_d_first_connect"
        self.terminal_imei_dao = TerminalImeiDao(redis_mgr)
        self.unreply_msg_mgr = unreply_msg_mgr
        self.terminal_rpc = terminal_rpc
        self.op_log_dao =  op_log_dao

    
    def imei_status_func(self, data):
        logger.debug("imei_status_func:%s", data)
        try:
            entity = MsgImeiStatus()
            entity.from_json_str(data)
            if entity.status == MSG_IMEI_CONNECTED:
                self.terminal_imei_dao.add(entity.imei, entity.server_id)
            else:
                ret = self.terminal_imei_dao.remove(entity.imei, entity.server_id)
                if ret != REMOVE_SUCCESS:
                    logger.warn("remove imei ret:%d", ret)
        except Exception, e:
            logger.exception(e)

    def del_unreply_func(self, data):
        logger.debug("del_unreply_func:%s", data)
        try:
            entity = MsgDelUnReply()
            entity.from_json_str(data)
            self.unreply_msg_mgr.delete_unreply_msg(entity.sn, entity.imei)

        except Exception, e:
            logger.exception(e)

   
   
    def first_connect_func(self, data):
        logger.debug("first_connect_func:%s", data)
        try:
            entity = MsgFirstConnect()
            entity.from_json_str(data)
            msgs = self.unreply_msg_mgr.get_un_reply_msg_and_remove(entity.imei)
            IOLoop.instance().add_callback(self.unicast_to_terminal, entity.server_id, entity.imei,  msgs)
        except Exception, e:
            logger.exception(e)

    @gen.coroutine
    def unicast_to_terminal(self, server_id, imei,  msgs):
        for msg in msgs:
            ok = True
            try:
                ret = yield self.terminal_rpc.unicast(server_id,imei=imei, content=msg)
                if ret["status"] != error_codes.EC_SUCCESS:
                    ok = False          
            except Exception, e:
                logger.exception(e)
                ok = False
            ret_str = "send ok" if ok else "send fail"
            self.on_log("[broadcast_srv_d]s2c first connect send_data:%s ret:%s" %
                          (msg, ret_str), imei)
        
    def on_log(self, content, imei):
        logger.info("op_log content:%s imei:%s", content, imei)
        return self.op_log_dao.add_op_info(imei=unicode(imei), content=unicode(content))    

        
        

    def start(self):
        RedisQueueWorker(self.redis_mgr, SERVICE_MSG_IMEI_STATUS,
                         self.imei_status_key, self.imei_status_func).start()

        RedisQueueWorker(self.redis_mgr, SERVICE_DEL_UNREPLY,
                         self.del_unreply_key, self.imei_status_func).start()

        RedisQueueWorker(self.redis_mgr, SERVIVE_FIRST_CONNECT,
                         self.first_connect_key, self.first_connect_func).start()
