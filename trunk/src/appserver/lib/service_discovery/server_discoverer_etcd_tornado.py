# -*- coding: utf-8 -*-

import copy
import collections
import threading
import rw_lock
import logging
import etcd3
from tornado.ioloop import IOLoop
from server_discoverer import DiscoverServerInfo, IDiscover

from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
from tornado import gen
#当前服务ID分配路径
DISCOVER_ETCD_BASE_PATH = "/discover"
#服务器节点信息存放基础路径
DISCOVER_ETCD_SERVERS_PATH = DISCOVER_ETCD_BASE_PATH + "/servers"
#服务节点存储的TTL
DISCOVER_ETCD_SERVER_DEFAULT_TTL = 20

logger = logging.getLogger(__name__)
class EtcdDiscoverWithTornado(IDiscover):
    executor = ThreadPoolExecutor(5)
    def __init__(self, host, user_name="", passwd=""):

        self.etcd = etcd3.client(host=host)
        self.my_servers_info = {}
        self.server_info = collections.defaultdict(dict)
        self.name_roundrobin_states = {}
        self.refresh_time = 5

    def _alloc_id(self, ip, port):
        ret = 0
        ip_to_int = lambda x:sum([256**j*int(i) for j,i in enumerate(x.split('.')[::-1])])
        int_ip = ip_to_int(ip)
        ret |= int_ip << 24
        ret |= port
        ret |= 0x0100000000000000
        return ret

    def get_by_name_roundrobin(self, name):
        ret = None
        sinfo_dict = self.server_info.get(name, None)
        if sinfo_dict is not None and len(sinfo_dict) > 0:
            rbst = self.name_roundrobin_states.get(name, 0)
            rbst += 1
            ret = sinfo_dict.values()[(len(sinfo_dict) + rbst) %
                                      len(sinfo_dict)]
            self.name_roundrobin_states[name] = rbst
        return ret

    def get_by_id(self, name, id):
        ret = self.server_info[name].get(id, None)   
        return ret

    def get_by_name(self, name):
        ret = None
        sinfo_dict = self.server_info.get(name, None)
        if sinfo_dict is not None and len(sinfo_dict) > 0:
            ret = sinfo_dict.values()
        return ret


    def register(self, sinfo):
        sinfo.id = self._alloc_id(sinfo.ip, sinfo.port)
        path = self._generate_server_path(sinfo.name, sinfo.id)
        lease = self.etcd.lease(sinfo.ttl)
        sinfo.lease_id = lease.id
        val = sinfo.to_json()
        self.etcd.put(path, val, lease)
        self.my_servers_info[sinfo.name] = sinfo
        return sinfo

    @gen.coroutine
    def refresh(self):
        my_servers_info = copy.deepcopy(self.my_servers_info)
        result = yield self._real_refresh(my_servers_info)
        IOLoop.current().call_later(self.refresh_time, self.refresh )
        

    def remove(self, name, id):
        self.server_info[name].pop(id, None)


    def dump_server_info(self):
        for k, v in self.server_info.items():
            logger.debug("name:%s item:%s", k, v)

    def update(self, sinfo):
        self.server_info[sinfo.name][sinfo.id] = sinfo

    def watch_call_back(self, event):
        event_type = type(event)
        logger.info("event:%s ", str(event))
        if event_type == etcd3.events.PutEvent:
            sinfo_str = event.value
            sinfo = DiscoverServerInfo(sinfo_str)
            self.update(sinfo)
            logger.info("server connected server_info:%s",
                        self.server_info)
        elif event_type == etcd3.events.DeleteEvent:
            key = event.key
            items = key.split("/")
            if len(items) != 5:
                logger.error("key arg error")
            self.remove(items[3], int(items[4]))
        self.dump_server_info()


    def _get_event(self):
        logger.info("get event start")
        events_iterator, cancel = self.etcd.watch_prefix(
            DISCOVER_ETCD_SERVERS_PATH)
        for event in events_iterator:
            IOLoop.instance().add_callback(self.watch_call_back, event)
            
    @run_on_executor
    def _get_prefix(self, path):
        ret = []
        for server in self.etcd.get_prefix(path):
            ret.append(server)
        return ret
    
    @run_on_executor
    def _real_refresh(self, my_server_infos):
        items = []
        for _, v in my_server_infos.items():
            try:
                for item in self.etcd.refresh_lease(v.lease_id):
                    items.append(item)
                    #logger.debug("refresh item:%s", str(item))
            except Exception, e:
                logger.exception(e)
        return items
        

    @gen.coroutine
    def watch(self):
        servers =  yield self._get_prefix(DISCOVER_ETCD_SERVERS_PATH)
        for server in servers:
            sinfo_str = server[0]
            sinfo = DiscoverServerInfo(sinfo_str)
            self.update(sinfo)
        threading.Thread(name="watch",
                         target=self._get_event).start()
        IOLoop.current().call_later(self.refresh_time, self.refresh )
        return 

    def _generate_server_name_path(self, name):
        return DISCOVER_ETCD_SERVERS_PATH + "/" + name

    def _generate_server_path(self, name, id):
        return "%s/%s/%d" % (DISCOVER_ETCD_SERVERS_PATH, name, id)
        
