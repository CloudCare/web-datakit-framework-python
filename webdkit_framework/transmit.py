import requests
import gzip
import datetime
import hashlib
import hmac
import base64
import threading
import time
import logging
from webdkit_framework.const import *

class DwTransport:
    transport_required_para = [DW_URL, DW_BATCHSIZE]
    transport_authen_para   = [DW_AK, DW_PK]
    content_type = "text/plain"
    content_coding = "gzip"

    def __init__(self, **kwargs):
        self.transport_kwargs = dict()
        self._authenticate = True
        self._data_list = []
        self._mutext = threading.RLock()

        for name in self.transport_required_para:
            assert name in kwargs.keys(), (
                "Expected a `{}` key-word parameter in `{}` configuration section, but it missed.".format(name, DW)
            )
            self.transport_kwargs[name] = kwargs.pop(name)

        for name in self.transport_authen_para:
            if name not in kwargs.keys():
                self._authenticate = False
                break
            self.transport_kwargs[name] = kwargs.pop(name)

        self.transport_kwargs[DW_FLUSHTIME] = kwargs.get(DW_FLUSHTIME, 0)
        if self.transport_kwargs[DW_BATCHSIZE] > 1 and self.transport_kwargs[DW_FLUSHTIME] != 0:
            self._flush_thread = threading.Thread(target=self._flush_task)
            self._flush_thread.setDaemon(True)
            self._flush_thread.start()
            logging.info("DwTransport flush thread start")

    def send(self, data):
        with self._mutext:
            self._data_list.append(data)
            if len(self._data_list) >= self.transport_kwargs.get(DW_BATCHSIZE):
                self.flush()

    def flush(self):
        is_found = False
        points = 0
        with self._mutext:
            points = len(self._data_list)
            if points > 0:
                is_found = True
                data = "".join(self._data_list)
                self._data_list = []
        if is_found:
            code = self._transport(data)
            logging.debug("DwTransport flush {} points, with http {} response".format(points, code))

    def _flush_task(self):
        while True:
            time.sleep(self.transport_kwargs[DW_FLUSHTIME])
            self.flush()

    def _transport(self, data):
        compress_data = gzip.compress(data.encode())
        header = self._build_http_heraer(compress_data)
        pesponse = requests.post(url = self.transport_kwargs.get("url"), headers = header,
                                 data = compress_data)
        return pesponse.status_code

    def _build_http_heraer(self, data):
        header = {}
        date = self._http_date()
        header["Content-Encoding"] = self.content_coding
        header["Content-Type"]     = self.content_type
        header["Date"]             = date
        if self._authenticate:
            header["Authorization"] = self._makeAuthorization(data, date)
        return header

    def _makeAuthorization(self, data, date):
        signature = "DWAY " + self.transport_kwargs[DW_PK] + ":"
        cont_md5 = hashlib.md5(data).digest()
        cont_md5 = base64.standard_b64encode(cont_md5).decode()
        s = "POST" + "\n" + cont_md5 + "\n" + self.content_type + "\n" + date
        return signature + self._hash_hmac(self.transport_kwargs[DW_AK], s)

    def _hash_hmac(self, key, code):
        hmac_code = hmac.new(key.encode(), digestmod=hashlib.sha1)
        hmac_code.update(code.encode())
        bs = hmac_code.digest()
        return base64.standard_b64encode(bs).decode()

    def _http_date(self):
        dt = datetime.datetime.utcnow()
        weekday = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]
        month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
                 "Oct", "Nov", "Dec"][dt.month - 1]
        return "{}, {:02d} {} {:4d} {:02d}:{:02d}:{:02d} GMT".format(weekday, dt.day, month,
                                                        dt.year, dt.hour, dt.minute, dt.second)