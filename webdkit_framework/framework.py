import os
import toml
import requests
import time
import logging
import queue
import threading
import json
import base64
from urllib.parse import urlencode
import nsq
from webdkit_framework.const import *
from webdkit_framework.transmit import DwTransport
from webdkit_framework.check import Checker
from webdkit_framework.protocol import LineProtoBuilder
from webdkit_framework.registry import Registry
from webdkit_framework.thread import AuthStage0Thread

class WdkBase(object):
    def init(self, toml_file):
        self._cfg = None
        self._toml_file = toml_file
        self._log_init(**self.cfg[LOG])
        self._dw_sender = DwTransport(**self.cfg[DW])
        self._checker   = Checker()
        self._builder   = LineProtoBuilder()
        Registry.reg_check()

        logging.info("WebDKit Start")
        logging.info("Dataway Parameters: {}".format(self.cfg[DW]))

    @property
    def cfg(self):
        if self._cfg == None:
            self._cfg = toml.load(self._toml_file)
        return self._cfg

    def get_url(self, topic):
        p = {}
        p[SUPERTOPIC] = self.cfg[NSQ][NSQ_SUPER_TOPIC]
        p[TOPIC] = topic
        base_url = self.cfg[WDF][WDF_HOST]
        base_url = base_url.strip()
        base_url = base_url.strip("/")
        if not base_url.startswith("http://"):
            base_url = "http://"+base_url
        base_url = "/".join([base_url, self.cfg[WDF][WDF_ROUTE]])
        base_url += "?"
        return base_url + urlencode(p)

    def _log_init(self, **kwargs):
        log_filename = ""
        if kwargs[LOG_FILE] != "":
            now = time.localtime(time.time())
            log_filename, ext_name = os.path.splitext(kwargs[LOG_FILE])
            log_filename += time.strftime("_%Y%m%d-%H%M%S", now)
            log_filename = "".join([log_filename, ext_name])
        log_level = kwargs[LOG_LEVEL].lower()
        level = logging.INFO
        if log_level == "debug":
            level = logging.DEBUG
        elif log_level == "info":
            level = logging.INFO
        elif log_level == "warn":
            level = logging.WARN
        elif log_level == "error":
            level = logging.ERROR
        elif log_level == "critical":
            level = logging.CRITICAL

        logging.basicConfig(filename=log_filename, format='%(asctime)s %(filename)s:%(lineno)d [%(levelname)s]:%(message)s',
                            filemode='w', level=level)
        logging.info("log file is {}".format(log_filename))

    def _get_reg_attr(self, name):
        if name != None:
            return getattr(self, name, None)
        return None

    def _process_exception(self, e):
        if self.cfg[EXCEPTION_ABORT]:
            logging.critical("app exit with exception {}".format(e))
            os._exit(1)

    def _process_auth(self, reg_element, topic, data, status):
        if status != AUTH_NONE_STATE and data != None:
            self.send_auth(topic, data, reg_element.auth_stage)

        if status == AUTH_FINISH_STATE or status == AUTH_NONE_STATE:
            reg_element.auth_stage = status
            self.send_auth(topic, None, reg_element.auth_stage)

        if status == AUTH_FINISH_STATE:
            logging.info("topic {} finish auth".format(topic))
        elif status == AUTH_NONE_STATE:
            logging.error("topic {} auth fail".format(topic))

    def send_wdf(self, data, topic):
        url = self.get_url(topic)
        try:
            r = requests.post(url=url, data=data)
        except requests.exceptions.ConnectionError as e:
            logging.error(e)
            return
        logging.debug("topic {} send_wdf: {} with response {} code".format(topic, url, r.status_code))
        if r.status_code != 200:
            logging.error("topic {} send_wdf: {} with response {} code".format(topic, url, r.status_code))

    def send_auth(self, topic, data=None, auth_stage=None):
        url = self.get_url(topic)
        param = {}
        param[AUTH] = auth_stage
        try:
            r = requests.post(url=url, data=data, params=param)
        except requests.exceptions.ConnectionError as e:
            logging.error(e)
            return 400
        logging.debug("topic {} send_auth: {} with response {} code".format(topic, url, r.status_code))
        return r.status_code

    def send_dway(self, data, topic):
        for d in data:
            if "timestamp" not in d:
                d["timestamp"] = int(time.time()*1E9)
            if not self._checker.check(**d):
                logging.error("Check Point {} fail".format(d))
                continue
            line_data = self._builder.build(**d)
            logging.debug("topic {} build line protocol:{}".format(topic, line_data))
            self._dw_sender.send(line_data)

    def start_request(self):
        def request_get_task(self, topic, interval, request_func):
            while True:
                logging.debug("topic {} period get".format(topic))
                try:
                    data = request_func()
                except Exception as e:
                    logging.error(e)
                    self._process_exception(e)
                    continue
                else:
                    if data != None:
                        self.send_wdf(data, topic)
                time.sleep(interval)

        for topic, reg_element in Registry.registries.items():
            request_func = self._get_reg_attr(reg_element.request_name)
            if request_func == None:
                continue
            t = threading.Thread(target=request_get_task, args=(self, topic, reg_element.interval, request_func))
            t.setDaemon(True)
            t.start()
            logging.info("topic {} period get thread start".format(topic))

    def start_auth_stage0(self):
        def auth_stage0_task():
            def task(topic, data, auth_stage, func):
                while True:
                    code = self.send_auth(topic, None, auth_stage)  # 向wdf申请topic
                    if code == 400:  # wdf上topic已在init状态，等待5秒后再尝试
                        time.sleep(5)
                        continue
                    elif code == 201:  # wdf上topic已在succ状态
                        return None, -3
                    elif code == 200:  # wdf上topic申请成功
                        try:
                            return func(data, auth_stage)
                        except Exception as e:
                            logging.error(e)
                            self._process_exception(e)
                            return None, -1

            tasks = []
            for topic, reg_element in Registry.registries.items():
                auth_func = self._get_reg_attr(reg_element.auth_name)
                if auth_func == None:
                    continue
                reg_element.auth_stage = AUTH_INIT_STATE
                logging.info("topic {} auth start".format(topic))
                t = AuthStage0Thread(func=task, args=(topic, None, reg_element.auth_stage, auth_func))
                tasks.append((t, topic, reg_element))
                t.setDaemon(True)
                t.start()
            for t_info in tasks:
                topic = t_info[1]
                task = t_info[0]
                reg_element = t_info[2]
                task.join()
                data, status = task.get_result()
                self._process_auth(reg_element, topic, data, status)

        t = threading.Thread(target=auth_stage0_task)
        t.setDaemon(True)
        t.start()

    def start_response(self):
        def start_response(self, topic, response_func, msg_q):
            while True:
                data = msg_q.get()
                logging.debug("topic {} recieve data".format(topic))
                try:
                    processed_data = response_func(data)
                except Exception as e:
                    logging.error(e)
                    self._process_exception(e)
                    continue
                else:
                    self.send_dway(processed_data, topic)

        for topic, reg_element in Registry.registries.items():
            response_func = self._get_reg_attr(reg_element.response_name)
            if response_func == None:
                continue
            msg_q = reg_element.msgq_resp = queue.Queue(128)
            t = threading.Thread(target=start_response, args=(self, topic, response_func, msg_q))
            t.setDaemon(True)
            t.start()

    def start_nsq(self):
        def parse_json_data(body):
            d = json.loads(body.decode())
            data_type = d["type"]
            http_data = base64.standard_b64decode(d["content"])
            http_decode_split = http_data.decode().split("\r\n")
            http_first = http_decode_split[0]
            http_body = http_decode_split[-1]
            url = http_first.split()[1]
            params = url.split("?")[1].split("&")
            params = {p.split("=")[0]: p.split("=")[1] for p in params}
            return data_type, params[SUPERTOPIC], params[TOPIC], http_data, http_body.encode()

        def send_msgq(msgq, data, topic):
            if msgq:
                if msgq.full():
                    logging.critical("topic {} msg queue full".format(topic))
                else:
                    msgq.put(data)
            else:
                logging.error("topic {} msgq not founed".format(topic))

        def auth_data_confirm(reg_element, topic, http_data):
            auth_func = self._get_reg_attr(reg_element.auth_name)
            if reg_element.auth_stage < 0:
                return
            reg_element.auth_stage += AUTH_STATE_UPDATA_STEP
            try:
                confirm_data, status = auth_func(http_data, reg_element.auth_stage)
            except Exception as e:
                logging.error(e)
                self._process_exception(e)
            else:
                self._process_auth(reg_element, topic, confirm_data, status)

        def nsq_handle(msg):
            data_type, super_topic, topic, http_data, http_body = parse_json_data(msg.body)
            logging.debug("nsq recieve type {} supertopic {} topic {}".format(data_type, super_topic, topic))
            if super_topic != self.cfg[NSQ][NSQ_SUPER_TOPIC]:
                logging.error("nsq receive supertopic {} unmatch with {}".format(super_topic, self.cfg[NSQ][NSQ_SUPER_TOPIC]))
                return True

            reg_element = Registry.registries.get(topic, None)
            if reg_element == None:
                logging.error("topic {} is not registry".format(topic))
                return True

            if data_type == DATA_TYPE_AUTH:
                auth_data_confirm(reg_element, topic, http_data)
            else:
                if reg_element.auth_name != None:
                    send_msgq(reg_element.msgq_resp, http_data, topic)
                else:
                    send_msgq(reg_element.msgq_resp, http_body, topic)
            return True

        nsq_cfg = self.cfg[NSQ]
        nsq.Reader(message_handler=nsq_handle, lookupd_http_addresses=[nsq_cfg[NSQ_HTTP_ADDR]],
                   topic=nsq_cfg[NSQ_SUPER_TOPIC], channel=nsq_cfg[NSQ_SUPER_CHNEL], max_in_flight=9)
        logging.info("nsq agent start")
        nsq.run()

    def run(self):
        self.start_response()
        self.start_request()
        self.start_auth_stage0()
        time.sleep(1)
        self.start_nsq()