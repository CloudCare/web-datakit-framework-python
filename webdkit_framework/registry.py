import logging
from webdkit_framework.const import AUTH_NONE_STATE

class RegElement:
    def __init__(self):
        self.topic    = None
        self.interval = None
        self.request_name  = None
        self.auth_name     = None
        self.auth_stage    = AUTH_NONE_STATE
        self.response_name = None
        self.msg_q = None
        self.msgq_auth = None
        self.msgq_resp = None

    def __str__(self):
        return "{"+"topic:{}, interval:{}, request_name:{}, auth_name:{}, response_name:{}, msgq_auth:{}, msgq_resp:{}".format(self.topic,
            self.interval, self.request_name, self.auth_name, self.response_name, self.msgq_auth, self.msgq_resp)+"}"


class Registry:
    registries = dict()

    @classmethod
    def reg_request(cls, topic, interval, request_name):
        reg_element = cls.registries.get(topic, RegElement())
        reg_element.topic = topic
        reg_element.interval = interval
        reg_element.request_name = request_name
        cls.registries[topic] = reg_element

    @classmethod
    def reg_response(cls, topic, response_name):
        reg_element = cls.registries.get(topic, RegElement())
        reg_element.topic = topic
        reg_element.response_name = response_name
        cls.registries[topic] = reg_element

    @classmethod
    def reg_auth(cls, topic, auth_name):
        reg_element = cls.registries.get(topic, RegElement())
        reg_element.topic = topic
        reg_element.auth_name = auth_name
        cls.registries[topic] = reg_element

    @classmethod
    def reg_check(cls):
        for k, v in cls.registries.items():
            cls.reg_chk_str(v.topic, "topic `{}` must be string".format(k))
            if v.interval:
                cls.reg_chk_int(v.interval, "topic `{}` interval  must be integer".format(k))
            if v.auth_name and v.request_name:
                err_str = "topic `{}` {} conflict with {}".format(k, v.auth_method_name, v.get_method_name)
                logging.error(err_str)
                raise RuntimeError(err_str)

    @classmethod
    def reg_chk_str(cls, val, err_str):
        if not isinstance(val, str):
            logging.error(err_str)
            raise RuntimeError(err_str)

    @classmethod
    def reg_chk_int(cls, val, err_str):
        if not isinstance(val, int):
            logging.error(err_str)
            raise RuntimeError(err_str)