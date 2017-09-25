# -*- coding: utf-8 -*-

from tornado import gen

from device_mongo_dao import DeviceMongoDAO


class DeivceDAO:
    @staticmethod
    def new(*args, **kwargs):
        mongo_dao = DeviceMongoDAO(*args, **kwargs)
        inst = DeivceDAO(db_dao = mongo_dao)
        return inst
    
    def __init__(self, **kwargs):
        self._db_dao = kwargs["db_dao"]
    
    def __getattr__(self, attr):
        return getattr(self._db_dao, attr)

