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
import logging
from lib.console import Console
from lib.pyloader import PyLoader
from lib.auth_dao import AuthDAO
from lib.files_dao import FilesDAO
from lib.user_dao import UserDAO
from lib.sys_config import SysConfig

import handlers.user.upload_logo
import handlers.get

from lib import sys_config, discover_config
from lib.service_discovery import server_discoverer_worker
from lib.mongo_dao_base import GetMongoClientAndAuth
from concurrent.futures import ThreadPoolExecutor
from lib.service_discovery import server_discoverer_worker
from lib import discover_config
logger = logging.getLogger(__name__)
define("debug_mode", 0, int,
       "Enable debug mode, 1 is local debug, 2 is test, 0 is disable")
define("port", 9700, int, "Listen port, default is 9700")
define("address", "0.0.0.0", str, "Bind address, default is 127.0.0.1")
define("console_port", 9710, int, "Console listen port, default is 9710")
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


worker = server_discoverer_worker.ServerDiscovererWorker()

#
thread_pool = ThreadPoolExecutor(max_thread_count)
mongo_client = GetMongoClientAndAuth(mongo_conf.default_meta)



# Init web application
webapp = Application(
    [
        (r"/file/pet/upload_logo", handlers.user.upload_logo.UploadLogo),
        (r"/file/get", handlers.get.Get),
    ],
    autoreload=False,
    pyloader=pyloader,
    files_dao=FilesDAO.new( mongo_client, thread_pool),
    auth_dao=AuthDAO.new( mongo_client, thread_pool),
    user_dao=UserDAO.new( mongo_client, thread_pool),
    appconfig=conf, )


class _UserSrvConsole(Console):
    def handle_cmd(self, stream, address, cmd):
        if len(cmd) == 1 and cmd[0] == "quit":
            self.send_response(stream, "Byte!")
            return False
        elif len(cmd) == 0:
            pass
        elif len(cmd) == 1 and cmd[0] == "reload-config":
            newconf = pyloader.ReloadInst("Config")
            webapp.settings["appconfig"] = newconf
            self.send_response(stream, "done")
        elif len(cmd) == 1 and cmd[0] == "reload-sysconfig":
            webapp.settings["sysconfig"].reload()
            self.send_response(stream, "done")
        else:
            self.send_response(stream, "Invalid command!")
        return True

# Init console
console = _UserSrvConsole()
console.bind(options.console_port, "127.0.0.1")
console.start()


# Init async
@gen.coroutine
def _async_init():
    SysConfig.new(sys_config.DEFAULT_CATEGORY, mongo_client, thread_pool)
    yield SysConfig.current().open()


try:
    worker.register(discover_config.FILE_SRV_D, options.port, 0, None)
    worker.work()
except Exception, e:
    print "worker register error exception:", e
    logger.exception(e)
    exit(0)


ioloop.IOLoop.current().run_sync(_async_init)

# Run web app loop
webapp.listen(options.port, options.address, xheaders=True)
ioloop.IOLoop.current().start()
