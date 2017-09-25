# -*- coding: utf-8 -*-

"""import copy
import collections
import threading
import rw_lock
import logging
import etcd3

from server_discoverer import DiscoverServerInfo, IDiscover
logger = logging.getLogger(__name__)

#当前服务ID分配路径
DISCOVER_ETCD_BASE_PATH = "/discover"
#服务器节点信息存放基础路径
DISCOVER_ETCD_SERVERS_PATH = DISCOVER_ETCD_BASE_PATH + "/servers"
#服务节点存储的TTL
DISCOVER_ETCD_SERVER_DEFAULT_TTL = 20


class EtcdDiscover(IDiscover):

    def __init__(self, host, user_name="", passwd=""):

        self.etcd = etcd3.client(host=host)
        self.my_servers_info = {}
        self.server_info = collections.defaultdict(dict)
        self.name_roundrobin_states = {}
        self.mutex = rw_lock.ReadWriteLock()

    def alloc_id(self, ip, port):
        ret = 0
        ip_to_int = lambda x:sum([256**j*int(i) for j,i in enumerate(x.split('.')[::-1])])
        int_ip = ip_to_int(ip)
        ret |= int_ip << 24
        ret |= port
        ret |= 0x0100000000000000
        return ret

    def get_by_name_roundrobin(self, name):
        ret = None
        self.mutex.rlock()
        sinfo_dict = self.server_info.get(name, None)
        if sinfo_dict is not None and len(sinfo_dict) > 0:
            rbst = self.name_roundrobin_states.get(name, 0)
            rbst += 1
            ret = sinfo_dict.values()[(len(sinfo_dict) + rbst) %
                                      len(sinfo_dict)]
            self.name_roundrobin_states[name] = rbst
        self.mutex.runlock()
        return ret

    def get_by_id(self, name, id):
        self.mutex.rlock()
        ret = self.server_info[name].get(id, None)   
        self.mutex.runlock()
        return ret

    def get_by_name(self, name):
        ret = None
        self.mutex.rlock()
        sinfo_dict = self.server_info.get(name, None)
        if sinfo_dict is not None and len(sinfo_dict) > 0:
            ret = sinfo_dict.values()
        self.mutex.runlock()
        return ret

    def register(self, sinfo):
        sinfo.id = self.alloc_id(sinfo.ip, sinfo.port)
        path = self.generateServerPath(sinfo.name, sinfo.id)
        lease = self.etcd.lease(sinfo.ttl)
        sinfo.lease_id = lease.id
        val = sinfo.to_json()
        self.etcd.put(path, val, lease)
        self.mutex.lock()
        self.my_servers_info[sinfo.name] = sinfo
        self.mutex.unlock()
        return sinfo

    def refresh(self):
        my_servers_info = copy.deepcopy(self.my_servers_info)
        for _, v in my_servers_info.items():
            try:
                for item in self.etcd.refresh_lease(v.lease_id):
                    logger.debug("refresh item:%s", str(item))
            except Exception, e:
                logger.exception(e)
        threading.Timer(5, self.refresh).start()

    def remove(self, name, id):
        self.mutex.lock()
        self.server_info[name].pop(id, None)
        self.mutex.unlock()

    def dump_server_info(self):
        print "dump_server_info start"
        self.mutex.rlock()
        for k, v in self.server_info.items():
            logger.debug("name:%s item:%s", k, v)
        self.mutex.runlock()

    def update(self, sinfo):
        self.mutex.lock()
        self.server_info[sinfo.name][sinfo.id] = sinfo
        self.mutex.unlock()

    def watch(self):
        #get
        logger.info("watch start")
        servers = self.etcd.get_prefix(DISCOVER_ETCD_SERVERS_PATH)
        for server in servers:
            sinfo_str = server[0]
            sinfo = DiscoverServerInfo(sinfo_str)
            self.update(sinfo)

        events_iterator, cancel = self.etcd.watch_prefix(
            DISCOVER_ETCD_SERVERS_PATH)
        for event in events_iterator:
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
                    continue
                self.remove(items[3], int(items[4]))
            self.dump_server_info()

    def generateServerNamePath(self, name):
        return DISCOVER_ETCD_SERVERS_PATH + "/" + name

    def generateServerPath(self, name, id):
        return "%s/%s/%d" % (DISCOVER_ETCD_SERVERS_PATH, name, id)

"""