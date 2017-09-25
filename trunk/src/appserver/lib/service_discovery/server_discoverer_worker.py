import os
from server_discoverer import *
import threading

from server_discoverer_etcd_tornado import EtcdDiscoverWithTornado
import logging
logger = logging.getLogger(__name__)


class ServerDiscovererWorker(object):
    SERVER_IP_ENV_NAME = "SERVER_IP"
    SERVER_GROUP_ENV_NAME = "SERVER_GROUP"
    DISCOVER_DOMAIN = "discover.xiaomaoqiu.com"

    def __init__(self):
        self.ip = os.environ.get(self.SERVER_IP_ENV_NAME)
        group_str = os.environ.get(self.SERVER_GROUP_ENV_NAME, 0)
        if self.ip is None:
            raise ServerDiscovererException("arg error")
        self.group = int(group_str)
        self.discover_server = EtcdDiscoverWithTornado(self.DISCOVER_DOMAIN)

    def get_discover(self):
        return self.discover_server

    def register(self, name, port, proto_type, custom_fields):
        if name == "":
            raise ServerDiscovererException("arg error")
        sinfo = DiscoverServerInfo()
        sinfo.name = name
        sinfo.port = port
        sinfo.ip = self.ip
        sinfo.proto_type = proto_type
        sinfo.custom_fields = custom_fields
        sinfo.ttl = 10
        return self.discover_server.register(sinfo)

    def work(self):
        self.discover_server.watch()
