# -*- coding: utf-8 -*-

import traceback
import sys
sys.path.append("../../")
sys.path.append("../terminal_srv_d/")

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
from lib.pyloader import PyLoader
from lib.terminal_rpc import TerminalRPC
import setproctitle
import logging
from concurrent.futures import ThreadPoolExecutor
from lib.op_log_dao import OPLogDAO
from lib.service_discovery import server_discoverer_worker
from lib import discover_config
from lib.redis.redis_mgr import RedisMgr
from lib.redis.terminal_imei_dao import TerminalImeiDao
from expire_keys_mgr import ExpireKeysMgr
from broadcast_handlers import *
from redis_queue_watch import RedisQueueWatcher
from lib.mongo_dao_base import GetMongoClientAndAuth
define("port", default=9797, help="run on the given port", type=int)

ptitle = "broadcast_srv_d"
verbose = False
debug = False
max_thread_count = 30

logger = logging.getLogger(__name__)
redis_pyloader = PyLoader("configs.redis_config")
redis_conf = redis_pyloader.ReloadInst("RedisConfig", debug_mode=debug)
mongo_pyloader = PyLoader("configs.mongo_config")
mongo_conf = mongo_pyloader.ReloadInst("MongoConfig", debug_mode=debug)
if __name__ == '__main__':
    tornado.options.options.logging = "debug"
    tornado.options.parse_command_line()
    setproctitle.setproctitle(ptitle)
    worker = server_discoverer_worker.ServerDiscovererWorker()
    terminal_rpc = TerminalRPC(worker.get_discover())
    redis_mgr = RedisMgr(redis_conf.startup_nodes)
    thread_pool = ThreadPoolExecutor(max_thread_count)
    mongo_client = GetMongoClientAndAuth(mongo_conf.default_meta)
    op_log_dao= OPLogDAO.new(mongo_client, thread_pool)
    terminal_imei_dao = TerminalImeiDao(redis_mgr)
    unreply_msg_mgr = ExpireKeysMgr(redis_mgr, terminal_imei_dao,terminal_rpc, op_log_dao ).get_unreply_msg_mgr()
    webapp = Application(
        [(r"/unicast", UnicastHandler),  
        (r"/send_command_params", SendParamsCommandHandler),
        (r"/send_commandj03", SendCommandHandlerJ03),
        (r"/send_commandj13", SendCommandHandlerJ13), ],
                         terminal_rpc=terminal_rpc,
                         unreply_msg_mgr=unreply_msg_mgr,
                         op_log_dao = op_log_dao,
                         terminal_imei_dao=terminal_imei_dao, )


    try:
        worker.register(discover_config.BROADCAST_SRV_D, options.port, 0, None)
        worker.work()
    except Exception, e:
        print "worker register error exception:", e
        logger.exception(e)
        exit(0)

    try:
        RedisQueueWatcher(redis_mgr,unreply_msg_mgr, terminal_rpc, op_log_dao ).start()
    except Exception, e:
        print "watcher error exception:", e
        logger.exception(e)
        exit(0)

    webapp.listen(options.port)
    print "started"

    IOLoop.current().start()
