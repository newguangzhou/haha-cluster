# -*- coding: utf-8 -*-

import os
import logging
import json
import threading


class ServerDiscovererException(Exception):
    def __init__(self, message, *args):
        self._msg = message % tuple(args)

    def __str__(self):
        return self._msg


class DiscoverServerInfo:
    def __init__(self, json_str=None):
        args_dict = {}
        if json_str is not None:
            args_dict = json.loads(json_str)
        self.id = args_dict.get("id", 0)
        self.name = args_dict.get("name", "")
        self.ip = args_dict.get("ip", "")
        self.port = args_dict.get("port", 0)
        self.group = args_dict.get("group", 0)
        self.proto_type = args_dict.get("proto_type", 0)
        self.ttl = args_dict.get("ttl", 0)
        self.custom_fields = args_dict.get("custom_fields", None)
        self.lease_id = args_dict.get("lease_id", 0)

    def to_json(self):
        args = {
            "id": self.id,
            "name": self.name,
            "ip": self.ip,
            "port": self.port,
            "group": self.group,
            "proto_type": self.proto_type,
            "ttl": self.ttl,
            "custom_fields": self.custom_fields,
            "lease_id": self.lease_id
        }
        data = json.dumps(args, ensure_ascii=False, encoding='utf8')
        return data

    def __str__(self):
        return self.to_json()


class IDiscover(object):
    def get_by_name_roundrobin(self, name):
        raise NotImplementedError

    def get_by_name(self, name):
        raise NotImplementedError

    def get_by_id(self, name, id):
        raise NotImplementedError

    def register(self, sinfo):
        raise NotImplementedError

    def refresh(self):
        raise NotImplementedError

    def watch(self):
        raise NotImplementedError


if __name__ == '__main__':
    pass
