# -*- coding: utf-8 -*-

import sys
sys.path.append("../../")
sys.path.append("../terminal_srv_d/")
reload(sys)
sys.setdefaultencoding('utf-8')

#import setproctitle
from tornado import ioloop, gen
from tornado.web import Application, url

import tornado.options
from tornado.options import define, options

from lib.console import Console
from lib.pyloader import PyLoader
from lib.auth_dao import AuthDAO
from lib.user_dao import UserDAO
from lib.pet_dao import PetDAO
from lib.global_dao import GlobalDAO
#from lib.device_dao import DeivceDAO
from lib.sys_config import SysConfig
from lib.new_device_dao import NewDeviceDAO

from lib.gid_rpc import GIDRPC
from lib.msg_rpc import MsgRPC

from lib.boradcast_rpc import BroadcastRPC
from lib import sys_config, discover_config
from lib.service_discovery import server_discoverer_worker
from lib.mongo_dao_base import GetMongoClientAndAuth
from concurrent.futures import ThreadPoolExecutor
from lib.service_discovery import server_discoverer_worker
from lib import discover_config
import logging

logger = logging.getLogger(__name__)

support_setptitle = True
try:
    import setproctitle
except:
    support_setptitle = False

import handlers

define("debug_mode", 0, int,
       "Enable debug mode, 1 is local debug, 2 is test, 0 is disable")
define("port", 9100, int, "Listen port, default is 9100")
define("address", "0.0.0.0", str, "Bind address, default is 127.0.0.1")
define("console_port", 9110, int, "Console listen port, default is 9110")

# Parse commandline
tornado.options.parse_command_line()
max_thread_count = 30
# Init pyloader
pyloader = PyLoader("config")
conf = pyloader.ReloadInst("Config")

mongo_pyloader = PyLoader("configs.mongo_config")
mongo_conf = mongo_pyloader.ReloadInst("MongoConfig",
                                       debug_mode=options.debug_mode)

# Set process title
if support_setptitle:
    setproctitle.setproctitle(conf.proctitle)
#
worker = server_discoverer_worker.ServerDiscovererWorker()
msg_rpc = MsgRPC(worker.get_discover())
broadcast_rpc = BroadcastRPC(worker.get_discover())

#
thread_pool = ThreadPoolExecutor(max_thread_count)
mongo_client = GetMongoClientAndAuth(mongo_conf.default_meta)

# Init web application
webapp = Application(
    [
        (r"/user/get_verify_code", handlers.GetVerifyCode),
        (r"/user/push_message_cmd", handlers.PushMessageCmd),
        (r"/user/login", handlers.Login),
        (r"/user/register", handlers.Register),
        (r"/user/logout", handlers.Logout),
        (r"/user/regen_token", handlers.RegenToken),
        (r"/user/set_home_wifi", handlers.SetHomeWifi),
        (r"/user/set_home_location", handlers.SetHomeLocation),
        (r"/user/get_base_infomation", handlers.GetBaseInfo),
        (r"/user/suggest", handlers.Suggest),
        (r"/pet/location", handlers.PetLocation),
        (r"/pet/location_test", handlers.PetLocation2),
        (r"/pet/walk", handlers.PetWalk),
        (r"/pet/find", handlers.PetFind),
        (r"/pet/get_pet_type_info", handlers.PetTypeInfo),
        (r"/pet/get_pet_info", handlers.GetPetInfo),
        (r"/pet/get_pet_status", handlers.GetPetStatusInfo),
        (r"/pet/add_pet_info", handlers.AddPetInfo),
        (r"/pet/update_pet_info", handlers.UpdatePetInfo),
        (r"/pet/healthy/get_activity_info", handlers.GetActivityInfo),
        (r"/pet/healthy/get_sleep_info", handlers.GetSleepInfo),
        (r"/pet/healthy/summary", handlers.Summary),
        (r"/pet/healthy/set_sport_info", handlers.SetTargetStep),
        (r"/pet/activity", handlers.PetActivity),
        (r"/device/add_device_info", handlers.AddDeviceInfo),
        (r"/device/get_info", handlers.GetDeviceInfo),
        (r"/device/remove_device_info", handlers.RemoveDeviceInfo),
        (r"/device/set_sim_info", handlers.SetSimInfo),
        (r"/device/switch_light", handlers.SwitchLight),
        (r"/device/get_light_status", handlers.GetDeviceSwitchLightStatus),
        (r"/device/send_get_wifi_list_cmd", handlers.SendGetWifiListCmd),
        (r"/device/get_wifi_list", handlers.GetWifiList),
        (r"/device/reboot_device_cmd", handlers.RebootDeviceCmd),
        (r"/user/agree_policy", handlers.AgreePolicy),
        (r"/device/get_device_status", handlers.GetPetStatusInfo),
        (r"/app/get_config", handlers.AppConfig),
        (r"/user/set_outdoor_on_off", handlers.OutdoorOnOff),
        (r"/user/set_outdoor_wifi", handlers.SetOutdoorWifi),

    ],
    debug=True,
    autoreload=True,
    pyloader=pyloader,
    user_dao=UserDAO.new(mongo_client, thread_pool),
    global_dao=GlobalDAO.new(mongo_client, thread_pool),
    auth_dao=AuthDAO.new(mongo_client, thread_pool),
    pet_dao=PetDAO.new(mongo_client, thread_pool),
    device_dao=NewDeviceDAO.new(mongo_client, thread_pool),
    broadcast_rpc = broadcast_rpc,
    msg_rpc=msg_rpc,
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
            webapp.settings["gid_rpc"] = GIDRPC(newconf.gid_rpc_url)
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
    SysConfig.new(sys_config.DEFAULT_CATEGORY,mongo_client, thread_pool)
    yield SysConfig.current().open()
    webapp.settings["gid_rpc"] = GIDRPC(SysConfig.current().get(sys_config.SC_GID_RPC_URL))



try:
    worker.register(discover_config.USER_SRV_D, options.port, 0, None)
    worker.work()
except Exception, e:
    print "worker register error exception:", e
    logger.exception(e)
    exit(0)


ioloop.IOLoop.current().run_sync(_async_init)

# Run web app loop
webapp.listen(options.port, options.address, xheaders=True)
ioloop.IOLoop.current().start()
