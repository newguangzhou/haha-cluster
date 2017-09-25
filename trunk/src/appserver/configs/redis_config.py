# -*- coding: utf-8 -*-


class RedisConfig:
    def __init__(self, *args, **kwargs):
        debug_mode = 0
        if kwargs.has_key("debug_mode"):
            debug_mode = kwargs["debug_mode"]

        if debug_mode == 0:
            self.startup_nodes = [{"host": "10.29.58.129", "port": 7399},{"host": "10.135.255.58", "port": 7399}]
        else:
            self.startup_nodes = [{"host": "127.0.0.1", "port": 6379}]
