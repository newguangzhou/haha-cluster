# -*- coding: utf-8 -*-

import traceback
import sys
sys.path.append("../../")

reload(sys)
sys.setdefaultencoding("utf-8")

import time
import getopt
import signal
import tornado
import tornado.options
from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado.web import Application, url
from tornado import gen
from terminal_base import conn_mgr2, broadcast
from lib.pyloader import PyLoader
from lib.op_log_dao import OPLogDAO
from lib.new_device_dao import NewDeviceDAO
from lib.pet_dao import PetDAO
import terminal_handler
import http_handlers
import imei_timer
import unreply_msg2
from test_handler import CloseTcp
from lib.msg_rpc import MsgRPC
from lib.sys_config import SysConfig
from lib import sys_config, discover_config
from lib.service_discovery import server_discoverer_worker
from lib.mongo_dao_base import GetMongoClientAndAuth
from concurrent.futures import ThreadPoolExecutor

from lib.redis.redis_mgr import RedisMgr
from lib.redis.redis_queue_sender import RedisQueueSender

support_setptitle = True
ptitle = "terminal_srv_d"
listen_port = 5050
debug = False
http_listen_port = 5052
max_thread_count = 30
try:
    import setproctitle
except:
    support_setptitle = False

import logging

logger = logging.getLogger(__name__)
mongo_pyloader = PyLoader("configs.mongo_config")
mongo_conf = mongo_pyloader.ReloadInst("MongoConfig", debug_mode=debug)
redis_pyloader = PyLoader("configs.redis_config")
redis_conf = redis_pyloader.ReloadInst("RedisConfig", debug_mode=debug)

# Set process title
if support_setptitle:
    setproctitle.setproctitle(ptitle)
else:
    logger.warning(
        "System not support python setproctitle module, please check!!!")

if __name__ == '__main__':
    tornado.options.options.logging = "debug"
    tornado.options.parse_command_line()
    conn_mgr = conn_mgr2.ServerConnMgr()
    broadcastor = broadcast.BroadCastor(conn_mgr)
    imei_timer_mgr = imei_timer.ImeiTimer()
    unreply_msg_mgr = unreply_msg2.UnreplyMsgMgr2()
    worker = server_discoverer_worker.ServerDiscovererWorker()
    msg_rpc = MsgRPC(worker.get_discover())
    thread_pool = ThreadPoolExecutor(max_thread_count)
    mongo_client = GetMongoClientAndAuth(mongo_conf.default_meta)
    try:
        sinfo = worker.register(discover_config.TERMINAL_SRV_D, http_listen_port, 0,
                        None)
        worker.work()
    except Exception, e:
        print "worker register errxor exception:", e
        logger.exception(e)
        exit(0)
    redis_mgr = RedisMgr(redis_conf.startup_nodes)
    redis_queue_sender = RedisQueueSender(redis_mgr)
    handler = terminal_handler.TerminalHandler(
        conn_mgr,
        debug,
        imei_timer_mgr,
        op_log_dao=OPLogDAO.new(mongo_client, thread_pool),
        broadcastor=broadcastor,
        pet_dao=PetDAO.new(mongo_client, thread_pool),
        new_device_dao=NewDeviceDAO.new(mongo_client, thread_pool),
        msg_rpc=msg_rpc,
        redis_queue_sender=redis_queue_sender,
        server_id = sinfo.id,
        unreply_msg_mgr=unreply_msg_mgr, )

    conn_mgr.CreateTcpServer("", listen_port, handler)

    webapp = Application([
        (r"/op_log", http_handlers.GetOpLogHandler),
        (r"/unicast", http_handlers.UnicastHandler),
        #(r"/send_command", http_handlers.SendCommandHandler),
        #(r"/send_command2", http_handlers.SendCommandHandler2),
        #(r"/send_command3", http_handlers.SendCommandHandler3),
        #(r"/send_command4", http_handlers.SendCommandHandler4),
        #(r"/send_command_params", http_handlers.SendParamsCommandHandler),
        #(r"/send_commandj03", http_handlers.SendCommandHandlerJ03),
        #(r"/send_commandj13", http_handlers.SendCommandHandlerJ13),
        #(r"/closesocket_byimei", CloseTcp)
    ],
                         broadcastor=broadcastor,
                         msg_rpc=msg_rpc,
                         unreply_msg_mgr=unreply_msg_mgr,
                         conn_mgr=conn_mgr,
                         op_log_dao=OPLogDAO.new(mongo_client, thread_pool), )

    webapp.listen(http_listen_port)
    imei_timer_mgr.set_on_imeis_expire(handler._OnImeiExpires)
    imei_timer_mgr.start()
    #unreply_msg_mgr.set_on_un_reply_msg_retry_func(handler._OnUnreplyMsgsSend)


    print "started"

    IOLoop.current().start()
