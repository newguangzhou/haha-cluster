# -*- coding: utf-8 -*-

import sys
sys.path.append("../../")

reload(sys)
sys.setdefaultencoding('utf-8')

import setproctitle

from tornado import ioloop, gen
from tornado.web import Application, url

import tornado.options
from tornado.options import define, options

from lib.console import Console
from lib.pyloader import PyLoader

from lib.auth_dao import AuthDAO

from sms_ymrt import YMRTSMS
from sms_nexmo import NEXMOSMS
from mipush import MIPush
from mipush2 import MiPush2
from sms_dayu import send_verify,send_message
import handlers

from lib import sys_config, discover_config
from lib.service_discovery import server_discoverer_worker
from lib.mongo_dao_base import GetMongoClientAndAuth
from concurrent.futures import ThreadPoolExecutor
from lib.service_discovery import server_discoverer_worker
from lib import discover_config
import logging
logger = logging.getLogger(__name__)
define("debug_mode", 0, int,
       "Enable debug mode, 1 is local debug, 2 is test, 0 is disable")
define("port", 9200, int, "Listen port, default is 9200")
define("address", "0.0.0.0", str, "Bind address, default is 127.0.0.1")
define("console_port", 9210, int, "Console listen port, default is 9210")
max_thread_count = 30
# Parse commandline
tornado.options.parse_command_line()

# Init pyloader
pyloader = PyLoader("config")
conf = pyloader.ReloadInst("Config")

mongo_pyloader = PyLoader("configs.mongo_config")
mongo_conf = mongo_pyloader.ReloadInst("MongoConfig",
                                       debug_mode=options.debug_mode)

# Set process title
setproctitle.setproctitle(conf.proctitle)

# # Init sms
# sms_sender = NEXMOSMS(pyloader)
worker = server_discoverer_worker.ServerDiscovererWorker()

#
thread_pool = ThreadPoolExecutor(max_thread_count)
mongo_client = GetMongoClientAndAuth(mongo_conf.default_meta)

# Init web application
webapp = Application(
    [
        (r"/msg/send_sms", handlers.SendSMS),
        (r"/msg/send_verify_code", handlers.SendVerify),
        (r"/msg/push_android", handlers.PushAndrod),
        (r"/msg/push_all", handlers.PushAll),
        (r"/msg/push_ios", handlers.PushIOS),
        (r"/msg/push", handlers.Push)
    ],
    autoreload=True,
    pyloader=pyloader,
    appconfig=conf,
    sms_registered=True,
    auth_dao=AuthDAO.new(mongo_client, thread_pool),
    sms_sender=send_message,
    verify_sender=send_verify,
    xiaomi_push2= MiPush2(conf.mipush_appsecret_android, conf.mipush_pkg_name,
                          conf.mipush_appsecret_ios, conf.mipush_bundle_id, True),
    xiaomi_push=MIPush(conf.mipush_host, conf.mipush_appsecret_android,
                       conf.mipush_pkg_name))


try:
    worker.register(discover_config.MSG_SRV_D, options.port, 0, None)
    worker.work()
except Exception, e:
    print "worker register error exception:", e
    logger.exception(e)
    exit(0)


print "started"
# Run web app loop
webapp.listen(options.port, options.address, xheaders=True)
ioloop.IOLoop.current().start()
