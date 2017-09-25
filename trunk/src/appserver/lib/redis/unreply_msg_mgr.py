# -*- coding: utf-8 -*-
import time
import logging
from tornado.ioloop import IOLoop
from redis_timer import RedisTimer
logger = logging.getLogger(__name__)

UN_REPLY_MSG_PREFIX = "unreply_msg"
UN_REPLY_MSG_TAG = "um"
IMEI_SEQ_KEY_PREFIX = "imei_seq"
MSG_DICT_TIMEOUT = 30*24*60*60

#to改成lua脚本 保证原子性
class UnreplyMsgMgr:
    def __init__(self,
                 redis_mgr,
                 on_un_reply_msg_retry_func=None,
                 max_count=3,
                 key_expires=60):
        self.timer = RedisTimer(redis_mgr, self.on_exire_keys, "un_reply_msg")
        self.max_count = max_count
        self.on_un_reply_msg_retry_func = on_un_reply_msg_retry_func
        self.key_expires = key_expires
        self.redis_mgr= redis_mgr
        if self.on_un_reply_msg_retry_func != None:
            self.timer.start()

    def set_on_un_reply_msg_retry_func(self, on_un_reply_msg_retry_func):
        self.on_un_reply_msg_retry_func = on_un_reply_msg_retry_func
        if not self.timer.is_started():
            self.timer.start()

    def add_unreply_msg(self, sn, imei, msg, msg_type):
        logger.debug("add_unreply_msg sn:%s, imei:%s msg:%s msg_type:%s", sn,
                     imei, msg, msg_type)
        seq = self.get_seq_by_sn(sn)
        client = self.redis_mgr.get_client()
        msg_dict_key = "{%s}%s:%s:%s" %(UN_REPLY_MSG_TAG, UN_REPLY_MSG_PREFIX, imei, msg_type)
        client.hmset(msg_dict_key, {"msg":msg, "max_count":self.max_count, "seq":seq})
        client.expire(msg_dict_key, MSG_DICT_TIMEOUT)

        imei_seq_key = "{%s}%s:%s" %(UN_REPLY_MSG_TAG, IMEI_SEQ_KEY_PREFIX, imei)
        client.hset(imei_seq_key, seq, msg_type)
        self.timer.add_key(self._gen_timer_key(imei, seq), self.key_expires)

    def get_seq_by_sn(self, sn):
        return sn[-4:]

    def delete_unreply_msg(self, sn, imei):
        logger.debug("delete_unreply_msg sn:%s,imei:%s ", sn, imei)
        client = self.redis_mgr.get_client()    
        seq = self.get_seq_by_sn(sn)
        imei_seq_key = "{%s}%s:%s" %(UN_REPLY_MSG_TAG, IMEI_SEQ_KEY_PREFIX, imei)
        client.hget(imei_seq_key, seq)
        msg_info, msg_type = self._get_msg_by_seq(imei, seq)
        if msg_type is None:
            logger.warning("not found in imei_seq seq:%s imei:%s", seq, imei)
        else:
            if msg_info is None or msg_info[2] != seq:
                logger.warning("seq msg_info not found or found err may be expires seq:%s imei:%s ", seq, imei)
            else:
                msg_dict_key = "{%s}%s:%s:%s" %(UN_REPLY_MSG_TAG, UN_REPLY_MSG_PREFIX, imei, msg_type)
                client.delete(msg_dict_key)
            client.hdel(imei_seq_key, seq)

    def get_un_reply_msg_and_remove(self, imei):
        logger.debug("get_un_reply_msg imei:%s ", imei)
        msgs = []
        client = self.redis_mgr.get_client()  
        imei_seq_key = "{%s}%s:%s" %(UN_REPLY_MSG_TAG, IMEI_SEQ_KEY_PREFIX, imei)  
        imei_seq_dict = client.hgetall(imei_seq_key)
        msg_types = imei_seq_dict.values()
        msg_types = set(msg_types)
        for msg_type in msg_types:
            msg_dict_key = "{%s}%s:%s:%s" %(UN_REPLY_MSG_TAG, UN_REPLY_MSG_PREFIX, imei, msg_type)
            msg_info = client.hmget(msg_dict_key, "msg", "max_count", "seq")
            msg_info[1] = int(msg_info[1])
            if msg_info[0] is not None:
                msgs.append(msg_info[0])
        client.delete(imei_seq_key)
        return msgs

    def _get_msg_by_seq(self, imei, seq):
        logger.debug("_get_msg_and_count_by_seq imei:%s seq:%s", imei, seq)
        client = self.redis_mgr.get_client()    
        msg_info = None
        imei_seq_key = "{%s}%s:%s" %(UN_REPLY_MSG_TAG, IMEI_SEQ_KEY_PREFIX, imei)
        msg_type = client.hget(imei_seq_key, seq)
        if msg_type is not None:
            msg_dict_key = "{%s}%s:%s:%s" %(UN_REPLY_MSG_TAG, UN_REPLY_MSG_PREFIX, imei, msg_type)
            msg_info = client.hmget(msg_dict_key, "msg", "max_count", "seq")
            msg_info[1] = int(msg_info[1])
        return msg_info, msg_type

    def _gen_timer_key(self, imei, seq):
        return "%s:%s" %(imei, seq)

    def _split_timer_key(self, timer_key):
        tmp = timer_key.split(":")
        if len(tmp) != 2:
            return None, None
        return tmp[0], tmp[1]
         
    
    def on_exire_keys(self, keys):
        need_retry_msgs = []
        client = self.redis_mgr.get_client()    
        for key in keys:
            imei, seq = self._split_timer_key(key)
            if imei is None:
                continue
            msg_info, msg_type = self._get_msg_by_seq(imei, seq)
            if msg_info is None or msg_info[0] is None:
                logger.warning("on on_exire_keys  imei:%s seq:%s not exist",
                               imei, seq)
                continue
            if msg_info[2] == seq:
                if msg_info[1] > 0:
                    print "msg_info", msg_info, msg_info[1]
                    need_retry_msgs.append((seq, imei, msg_info[0], self.max_count - msg_info[1] + 1))
                    msg_dict_key = "{%s}%s:%s:%s" %(UN_REPLY_MSG_TAG, UN_REPLY_MSG_PREFIX, imei, msg_type)
                    client.hincrby(msg_dict_key, "max_count", -1)
                else:
                    logger.warning(
                        "on on_exire_keys  imei:%s seq:%s count:%d need client reconnect",
                        imei, seq, msg_info[1])

            else:
                logger.warning(
                    "on on_exire_keys imei:%s seq:%s not eq seq:%s may be replaced",
                    imei, seq, msg_info[2])

        if len(need_retry_msgs) > 0:
            self.on_un_reply_msg_retry_func(need_retry_msgs)
        for retry_msg in need_retry_msgs:
            self.timer.add_key(self._gen_timer_key(retry_msg[1], retry_msg[0]), self.key_expires)
