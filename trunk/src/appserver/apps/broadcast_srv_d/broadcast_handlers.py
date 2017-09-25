# -*- coding: utf-8 -*-
import tornado.web
from tornado import gen
from datetime import datetime
import urllib
from lib import utils, error_codes
from lib.xmq_http_handler import WithImeiLogHandlerMixin, WithBlockFuncHandler, BaseHttpHandler
from terminal_base import terminal_proto,terminal_packets
from test_data import TEST_S2C_COMMAND_DATA
import json 
import base64
import logging
logger = logging.getLogger(__name__)



JO3_HTML = """
<!DOCTYPE html>
<html>
<script src="http://git.oschina.net/loonhxl/jbase64/raw/master/jbase64.js" language="Javascript"></script>
<script language="Javascript">
function my_post(path, params, method) {
    method = method || "post"; // Set method to post by default if not specified.

    // The rest of this code assumes you are not using a library.
    // It can be made less wordy if you use one.
    var form = document.createElement("form");
    form.setAttribute("method", method);
    form.setAttribute("action", path);

    for(var key in params) {
        if(params.hasOwnProperty(key)) {
            var hiddenField = document.createElement("input");
            hiddenField.setAttribute("type", "hidden");
            hiddenField.setAttribute("name", key);
            hiddenField.setAttribute("value", params[key]);

            form.appendChild(hiddenField);
         }
    }

    document.body.appendChild(form);
    form.submit();
}
function commit_form(){
	 var imei = document.getElementById("imei").value;
	 var command_content = document.getElementById("command_content").value;
	 var base64 = BASE64.encoder(command_content);//返回编码后的字符 
	 my_post("/send_commandj03",{'imei':imei, 'command_content':base64}) 
}
</script>
<body>
<form id="form_1" >  
<br>
imei:<br>
<input type="text" name="imei" id="imei" size="30">
<br>
j03command:<br>
<input type="text" name="command_content" id="command_content" size="100"><br>
<input type="button" value="Submit" onclick='commit_form()'>
<form>  
</body>
</html>


"""


class UnicastHandler(WithBlockFuncHandler, WithImeiLogHandlerMixin):
    @gen.coroutine
    def do_request(self):
        status = error_codes.EC_SUCCESS
        imei = ""
        content = ""
        terminal_rpc = self.settings["terminal_rpc"]
        terminal_imei_dao = self.settings["terminal_imei_dao"]
        try:
            imei = self.get_argument("imei")
            content = self.get_argument("content")
            if imei == "" or content == "":
                status = error_codes.EC_INVALID_ARGS
        except Exception as e:
            logger.exception(e)
            status = error_codes.EC_INVALID_ARGS
        res = {}
        if status == error_codes.EC_INVALID_ARGS:
            res["status"] = status
        else:
            ok = True
            try:
                server_id = yield self.call_func(terminal_imei_dao,
                                                 "get_server_id", imei)
                ret = yield terminal_rpc.unicast(server_id,imei=imei, content=content)
                res["status"] = ret["status"]
                if res["status"] != error_codes.EC_SUCCESS:
                    ok = False          
            except Exception, e:
                status = error_codes.EC_SYS_ERROR
                res["status"]  = status
                logger.exception(e)
                ok = False
            ret_str = "send ok" if ok else "send fail"
            self.on_log("[broadcast_srv_d] s2c send_data:%s ret:%s" % (content, ret_str), imei)
        self.write_obj(res)
        

    def get(self):
        return self.do_request()

    def post(self):
        return self.do_request()


class SendParamsCommandHandler(WithBlockFuncHandler,
                               WithImeiLogHandlerMixin):
    @gen.coroutine
    def do_request(self):
        terminal_rpc = self.settings["terminal_rpc"]
        terminal_imei_dao = self.settings["terminal_imei_dao"]
        res = {"status": error_codes.EC_SUCCESS}
        try:
            imei = self.get_argument("imei")
            content = self.get_argument("command_content")
        except Exception as e:
            res["status"] = error_codes.EC_INVALID_ARGS
        else:
            pk = terminal_packets.SendCommandReq(imei, content)
            send_data = str(pk)
            ok = True
            try:
                server_id = yield self.call_func(terminal_imei_dao,
                                                 "get_server_id", imei)
                ret = yield terminal_rpc.unicast(server_id,imei=imei, content=send_data)
                res["status"] = ret["status"]
                if res["status"] != error_codes.EC_SUCCESS:
                    ok = False          
            except Exception, e:
                status = error_codes.EC_SYS_ERROR
                res["status"]  = status
                logger.exception(e)
                ok = False
            ret_str = "send ok" if ok else "send fail"
            self.on_log("[broadcast_srv_d] s2c send_data:%s ret:%s" % (send_data, ret_str), imei)
        self.write_obj(res)
        if res["status"] != error_codes.EC_INVALID_ARGS:
            unreply_msg_mgr = self.settings["unreply_msg_mgr"]
            msg_type = content[0:3]
            add_unreply_msg_ret = yield self.call_func(unreply_msg_mgr, "add_unreply_msg", pk.sn, imei, send_data, msg_type)
            logger.debug("add_unreply_msg_ret :%s", str(add_unreply_msg_ret))

    def get(self):
        return self.do_request()

    def post(self):
        return self.do_request()





class SendCommandHandlerJ13(WithBlockFuncHandler,
                            WithImeiLogHandlerMixin):
    @gen.coroutine
    def do_request(self):
        terminal_rpc = self.settings["terminal_rpc"]
        terminal_imei_dao = self.settings["terminal_imei_dao"]
        res = {"status": error_codes.EC_SUCCESS}
        try:
            imei = self.get_argument("imei")
        except Exception as e:
            res["status"] = error_codes.EC_INVALID_ARGS
        else:
            pk = terminal_packets.GetLocationAck(terminal_proto.GenSN(), imei)
            send_data = str(pk)
            ok = True
            try:
                server_id = yield self.call_func(terminal_imei_dao,
                                                 "get_server_id", imei)
                ret = yield terminal_rpc.unicast(server_id,imei=imei, content=send_data)
                res["status"] = ret["status"]
                if res["status"] != error_codes.EC_SUCCESS:
                    ok = False          
            except Exception, e:
                status = error_codes.EC_SYS_ERROR
                res["status"]  = status
                logger.exception(e)
                ok = False
            ret_str = "send ok" if ok else "send fail"
            self.on_log("[broadcast_srv_d] s2c send_data:%s ret:%s" % (send_data, ret_str), imei)

        self.write_obj(res)

    def get(self):
        return self.do_request()

    def post(self):
        return self.do_request()

    


class SendCommandHandlerJ03(WithBlockFuncHandler,
                        WithImeiLogHandlerMixin):
    @gen.coroutine
    def get(self):
        self.write(JO3_HTML)

    @gen.coroutine
    def post(self):
        terminal_rpc = self.settings["terminal_rpc"]
        terminal_imei_dao = self.settings["terminal_imei_dao"]
        try:
            imei = self.get_argument("imei")
            content = self.get_argument("command_content")
        except Exception as e:
            self.write("arg error ")
            return
        content = base64.decodestring(content)
        pk = terminal_packets.SendCommandReq(imei, content)
        send_data = str(pk)
        ok = True
        try:
            server_id = yield self.call_func(terminal_imei_dao,
                                                "get_server_id", imei)
            ret = yield terminal_rpc.unicast(server_id,imei=imei, content=send_data)
            if ret["status"] != error_codes.EC_SUCCESS:
                ok = False          
        except Exception, e:
            status = error_codes.EC_SYS_ERROR
            logger.exception(e)
            ok = False
        ret_str = "send ok" if ok else "send fail"
        self.on_log("[broadcast_srv_d] s2c send_data:%s ret:%s" % (send_data, ret_str), imei)
        self.write(ret_str)
        unreply_msg_mgr = self.settings["unreply_msg_mgr"]
        msg_type = content[0:3]
        add_unreply_msg_ret = yield self.call_func(unreply_msg_mgr, "add_unreply_msg", pk.sn, imei, send_data, msg_type)
        logger.debug("add_unreply_msg_ret :%s", str(add_unreply_msg_ret))


class SendCommandHandler3(WithBlockFuncHandler,
                        WithImeiLogHandlerMixin):
    @gen.coroutine
    def get(self):
        terminal_rpc = self.settings["terminal_rpc"]
        terminal_imei_dao = self.settings["terminal_imei_dao"]
        try:
            imei = self.get_argument("imei")
        except Exception as e:
            self.write("arg error ")
            return
        type = self.get_argument("type", "1")
        command_num = self.get_argument("command_num", "005")
        logger.info("imei:%s, type:%s command_num:%s", imei, type, command_num)
        if type == "0":
            command_pk = TEST_S2C_COMMAND_DATA.get(command_num, None)
            if command_pk is None:
                self.write("arg error ")
                return
            else:
                pk = terminal_packets.SendCommandReq(imei, command_pk)
        else:
            pk = terminal_packets.GetLocationAck(terminal_proto.GenSN(), imei)
        send_data = str(pk)
        ok = True
        try:
            server_id = yield self.call_func(terminal_imei_dao,
                                                "get_server_id", imei)
            ret = yield terminal_rpc.unicast(server_id,imei=imei, content=send_data)
            if ret["status"] != error_codes.EC_SUCCESS:
                ok = False          
        except Exception, e:
            status = error_codes.EC_SYS_ERROR
            logger.exception(e)
            ok = False
        ret_str = "send ok" if ok else "send fail"
        self.on_log("[broadcast_srv_d] s2c send_data:%s ret:%s" % (send_data, ret_str), imei)
        self.write(ret_str)
        if type == "0":
            unreply_msg_mgr = self.settings["unreply_msg_mgr"]
            add_unreply_msg_ret = yield self.call_func(unreply_msg_mgr, "add_unreply_msg", pk.sn, imei, send_data, command_num)
            logger.debug("add_unreply_msg_ret :%s", str(add_unreply_msg_ret))

