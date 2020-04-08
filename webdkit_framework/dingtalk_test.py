import random
import requests
import json
from dingtalk.crypto import DingTalkCrypto
from webdkit_framework.framework import WdkBase
from webdkit_framework.decorator import Request, Response, Auth, UnAuth

class DingTalk(WdkBase):
    def __init__(self):
        self.token = "123456"
        self.corp_id = "ding756c3a3731cc4d7c"
        self.app_key = "dingevjpdegyhmfvvzmx"
        self.app_secret = "-sF4tzkZ9dE17owMcYINj34_HJQf4eViUuzT9aZbRfbYjUpIU7ZzPAt9dQjK4OL6"
        self.access_token = self.get_access_token()
        self.ads_encoding_key = "rvEljT9ZH5IOWM4QosIzyYvcBl9w4TeucaXqMTqWtYR"
        self.crypto = DingTalkCrypto(self.token, self.ads_encoding_key, self.corp_id)

    def gen_ads_encoding_key(self):
        char_set = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        letters = [random.choice(char_set) for _ in range(43)]
        return "".join(letters)

    def get_access_token(self):
        request = requests.get("https://oapi.dingtalk.com/gettoken?appkey={}&appsecret={}".format(self.app_key, self.app_secret))
        return request.json()["access_token"]

    def reg_ding_callback(self):
        headers = {"Content-Type": "application/json"}
        data = {}
        data['call_back_tag'] = ['bpms_task_change']
        data['token'] = self.token
        data['aes_key'] = self.ads_encoding_key
        data['url'] = self.get_url("bpms_task_change")

        json_data = json.dumps(data)
        url = "https://oapi.dingtalk.com/call_back/register_call_back?access_token={}".format(self.access_token)
        req = requests.post(url=url, data=json_data, headers=headers)
        json_reply = json.loads(req.text)
        print("---------------------------stage 0----------------------------")
        print(json_reply)
        if json_reply["errcode"] == 0 and json_reply["errmsg"] == "ok":
            return None, -2
        return None, -1

    def ding_talk_confrim(self, data):
        http_content = data.decode().split("\r\n")
        http_first = http_content[0]
        url = http_first.split()[1]
        print(http_content)
        params = url.split("?")[1].split("&")
        params = {p.split("=")[0]: p.split("=")[1] for p in params}

        signature = params["signature"]
        timestamp = params["timestamp"]
        nonce = params["nonce"]
        encrypt = http_content[-1]

        json_decryp = self.crypto.decrypt_message(msg=encrypt, signature=signature, timestamp=timestamp, nonce=nonce)
        decryp = json.loads(json_decryp)
        print("---------------------------stage 1----------------------------")
        print(decryp)
        event = decryp.get("EventType", "")
        if event != "check_url":
            return None, False

        confirm_data = self.crypto.encrypt_message(msg="success")
        print(confirm_data)
        return json.dumps(confirm_data).encode(), None

    @Auth(topic="bpms_task_change")
    def ding_talk_auth(self, data, auth_stage):
        if auth_stage == 0:
            return self.reg_ding_callback()
        elif auth_stage == 1:
            return self.ding_talk_confrim(data)

    @Response(topic="bpms_task_change")
    def ding_talk_process(self, data):
        print("---------------------------process----------------------------")
        http_content = data.decode().split("\r\n")
        http_first = http_content[0]
        url = http_first.split()[1]
        params = url.split("?")[1].split("&")
        params = {p.split("=")[0]: p.split("=")[1] for p in params}

        signature = params["signature"]
        timestamp = params["timestamp"]
        nonce = params["nonce"]
        encrypt = http_content[-1]
        json_decryp = self.crypto.decrypt_message(msg=encrypt, signature=signature, timestamp=timestamp, nonce=nonce)
        decryp = json.loads(json_decryp)
        points = []
        pt = {}
        tags = {}
        tags["corpId"] = decryp["corpId"]
        tags["EventType"] = decryp["EventType"]
        tags["title"] = decryp["title"]
        tags["type"] = decryp["type"]
        fields = {}
        fields["processInstanceId"] = decryp["processInstanceId"]
        fields["businessId"] = decryp["businessId"]
        fields["processCode"] = decryp["processCode"]
        fields["bizCategoryId"] = decryp["bizCategoryId"]
        fields["staffId"] = decryp["staffId"]
        fields["taskId"] = decryp["taskId"]
        pt["measurement"] = "bpms_task_change"
        pt["tags"] = tags
        pt["fields"] = fields
        points.append(pt)
        return points

    @UnAuth(topic="bpms_task_change")
    def unauth(self):
        r = requests.get(url="https://oapi.dingtalk.com/call_back/delete_call_back?access_token={}".format(
            "7b902c128f203d9aa8972fb996f2d34a"))
        print(r.text)
        return True

    @Request(topic="gitlab_ft-2.0_commit", interval=300)
    def request_gitlab_ft_20_commit(self):
        param = {}
        param["private_token"] = self.cfg["gitlab"]["token"]  # 获取配置文件中gitlab token信息
        r = requests.get(url="https://gitlab.jiagouyun.com/api/v4/projects/333/repository/commits/ft-2.0", params=param)
        return r.content  # 返回获取到数据，json格式

    @Response(topic="gitlab_ft-2.0_commit")
    def response_gitlab_ft_20_commit(self, data):
        points = []
        d = json.loads(data.decode())  # json解析

        fields = {}  # field填充
        fields["id"] = d["id"]
        fields["created_at"] = d["created_at"]
        fields["message"] = d["message"]

        tags = {}  # tag填充
        tags["title"] = d["title"]
        tags["author_name"] = d["author_name"]
        tags["author_email"] = d["author_email"]
        tags["project_id"] = str(d["project_id"])

        p = {}  # point构造
        p["measurement"] = "gitlab_ft_20_commit"
        p["tags"] = tags
        p["fields"] = fields
        points.append(p)

        return points


d = DingTalk()
d.init("webdkit.conf")
d.run()