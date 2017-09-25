# -*- coding: utf-8 -*-
from lib.redis.unreply_msg_mgr import UnreplyMsgMgr
from tornado import gen
import logging
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
from lib import utils, error_codes
logger = logging.getLogger(__name__)
class ExpireKeysMgr(object):
    executor = ThreadPoolExecutor(2)
    def __init__(self, redis_mgr, terminal_dao, terminal_rpc, op_log_dao):
        self.redis_mgr = redis_mgr
        self.terminal_dao = terminal_dao
        self.terminal_rpc = terminal_rpc
        self.op_log_dao = op_log_dao
        self.unreply_msg_mgr = UnreplyMsgMgr(redis_mgr, self.on_keys_expires)

    def get_unreply_msg_mgr(self):
        return self.unreply_msg_mgr

    @run_on_executor
    def get_server_id(self, imei):
        return  self.terminal_dao.get_server_id(imei)


    def on_log(self, content, imei):
        logger.info("op_log content:%s imei:%s", content, imei)
        return self.op_log_dao.add_op_info(imei=unicode(imei), content=unicode(content))    

    @gen.coroutine
    def on_keys_expires(self, reply_msgs):
        logger.debug("on_keys_expires reply_msgs:%s", str(reply_msgs))
        for sn, imei, msg, count in reply_msgs:
            server_id = yield self.get_server_id(imei)
            ok = True
            try:
                ret = yield self.terminal_rpc.unicast(server_id,imei=imei, content=msg)
                logger.debug(ret)
                if ret["status"] != error_codes.EC_SUCCESS:
                    ok = False          
            except Exception, e:
                logger.exception(e)
                ok = False
            ret_str = "send ok" if ok else "send fail"
            yield self.on_log("[broadcast_srv_d]s2c retry count:%d send_data:%s ret:%s" %
                          (count, msg, ret_str), imei)
        
        
    