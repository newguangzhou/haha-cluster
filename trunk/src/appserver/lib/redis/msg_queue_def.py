# -*- coding: utf-8 -*-
import time
import json
MSG_IMEI_CONNECTED = 1
MSG_IMEI_DISCONNECTED = 0


class MsgBase(object):
    def __init__(self):
        self.timestamp = int(time.time())

    def to_json_str(self):
        data = json.dumps(self.__dict__, ensure_ascii=False, encoding='utf8')
        return data

    def __str__(self):
        return self.to_json_str()

    def from_json_str(self, json_str):
        args_dict = json.loads(json_str)
        self.__dict__ = args_dict


class MsgImeiStatus(MsgBase):
    def __init__(self, imei=None, server_id=None, status=None):
        super(MsgImeiStatus, self).__init__()
        self.imei = imei
        self.server_id = server_id
        self.status = status

class MsgDelUnReply(MsgBase):
    def __init__(self, imei=None, sn=None):
        super(MsgDelUnReply, self).__init__()
        self.imei = imei
        self.sn = sn
        #self.msg_type = msg_type

class MsgFirstConnect(MsgBase):
    def __init__(self, imei=None, server_id=None):
        super(MsgFirstConnect, self).__init__()
        self.imei = imei
        self.server_id = server_id

SERVICE_MSG_IMEI_STATUS = "service_imei_status"
SERVICE_DEL_UNREPLY = "service_del_un_reply"
SERVIVE_FIRST_CONNECT = "service_first_connect"

def test(entity):
    entity.from_json_str("""{"imei": "123123122", "timestamp": 1504767576}""")
    return


def main():
    b = MsgImeiStatus()
    test(b)
    print b


if __name__ == '__main__':
    main()