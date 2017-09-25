# -*- coding: utf-8 -*-

import urllib
import time
import logging
import uuid
import json
import http_rpc
import traceback

from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from discover_config import BROADCAST_SRV_D
import type_defines
import error_codes


class BroadcastRPC(http_rpc.HttpRpc):
    def __init__(self, discover):
        http_rpc.HttpRpc.__init__(self, discover)
        self.name = BROADCAST_SRV_D


    @gen.coroutine
    def send_j13(self,imei):
        ret = yield self.call(self.name, "send_commandj13",imei=imei)
        raise gen.Return(ret)


    @gen.coroutine
    def send_command_params(self, **args):
        ret = yield self.call(self.name, "send_command_params", **args)
        raise gen.Return(ret)

    @gen.coroutine
    def unicast(self,**args):
        ret = yield self.call(self.name, "unicast",**args)
        raise gen.Return(ret)
