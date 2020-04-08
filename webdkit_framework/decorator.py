import logging
from webdkit_framework.registry import Registry
from webdkit_framework.const import AUTH_NONE_STATE

def Request(topic, interval):
    def request_outer(func):
        func_name = func.__name__
        Registry.reg_request(topic=topic, interval=interval, request_name=func_name)
        def request_inner(*args, **kwargs):
            return func(*args, **kwargs)
        return request_inner
    return request_outer

def Response(topic):
    def response_outer(func):
        func_name = func.__name__
        Registry.reg_response(topic=topic, response_name=func_name)
        def response_inner(*args, **kwargs):
            return func(*args, **kwargs)
        return response_inner
    return response_outer

def Auth(topic):
    def auth_outer(func):
        func_name = func.__name__
        Registry.reg_auth(topic=topic, auth_name=func_name)
        def auth_inner(*args, **kwargs):
            return func(*args, **kwargs)
        return auth_inner
    return auth_outer

def UnAuth(topic):
    def unauth_outer(func):
        def unauth_inner(*args, **kwargs):
            self = args[0]
            is_succ =  func(*args, **kwargs)
            if is_succ != True:
                return
            rc = self.send_auth(topic=topic, data=None, auth_stage=AUTH_NONE_STATE)
            if rc == 200:
                logging.info("topic {} unauth success".format(topic, rc))
            else:
                logging.error("topic {} unauth with response {} code".format(topic, rc))
        return unauth_inner
    return unauth_outer
