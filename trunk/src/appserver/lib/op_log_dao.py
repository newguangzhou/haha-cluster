# -*- coding: utf-8 -*-

from tornado import gen

from op_log_mongo_dao import OpLogMongoDAO


class OPLogDAO:
    @staticmethod
    def new(*args, **kwargs):
        mongo_dao = OpLogMongoDAO(*args, **kwargs)
        inst = OPLogDAO(db_dao=mongo_dao)
        return inst

    def __init__(self, **kwargs):
        self._db_dao = kwargs["db_dao"]

    def __getattr__(self, attr):
        return getattr(self._db_dao, attr)
