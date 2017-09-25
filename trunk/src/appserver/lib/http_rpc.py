# -*- coding: utf-8 -*-
from tornado import gen
from tornado import httpclient
import urllib
import time
import logging
import json
import socket
logger = logging.getLogger(__name__)


class HttpRPCException(Exception):
    def __init__(self, message, *args):
        self._message = message % tuple(args)

    def __str__(self):
        return self._message


class HttpRpc:
    def __init__(self, discover):
        self.discover = discover

    @gen.coroutine
    def call(self, name, path, id=None,**args):
        ret = None
        while True:
            if id is None:
                sinfo = self.discover.get_by_name_roundrobin(name)
            else:
                sinfo = self.discover.get_by_id(name, int(id))
            if sinfo is not None:
                url = "http://%s:%d/%s" % (sinfo.ip, sinfo.port, path)
                try:
                    ret = yield self._real_call(url, **args)
                except socket.error as ioerr:
                    logger.warning("call socket.error:%s", ioerr)
                    if ioerr.errno == 61:
                        self.discover.remove(sinfo.name, sinfo.id)
                        if id is not None:
                            continue
                        else:
                            raise ioerr
                    else:
                        raise ioerr
                except Exception, e:
                    logger.exception(e)
                    raise e
            else:
                self.discover.dump_server_info()
                raise HttpRPCException("no found server:%s id:%s" % (name, str(id)))
            break
        raise gen.Return(ret)

    @gen.coroutine
    def _real_call(self, url, **body):
        logger.debug("url:%s body:%s", url, str(body))
        ret = None
        http_client = httpclient.AsyncHTTPClient()

        res = yield http_client.fetch(url,
                                      method="POST",
                                      body=urllib.urlencode(body),
                                      connect_timeout=5,
                                      request_timeout=10)

        try:
            ret = json.loads(res.body)
        except Exception, e:
            logger.exception(e)
            ret = res.body

        raise gen.Return(ret)
