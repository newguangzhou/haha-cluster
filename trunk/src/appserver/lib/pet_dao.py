# -*- coding: utf-8 -*-

from tornado import gen

from pet_mongo_dao import PetMongoDAO


class PetDAO:
    @staticmethod
    def new(*args, **kwargs):
        mongo_dao = PetMongoDAO(*args, **kwargs)
        inst = PetDAO(db_dao = mongo_dao)
        return inst
    
    def __init__(self, **kwargs):
        self._db_dao = kwargs["db_dao"]

    def __getattr__(self, attr):
        return getattr(self._db_dao, attr)

