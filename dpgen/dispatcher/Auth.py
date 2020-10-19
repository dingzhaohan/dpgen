import uuid
import os
import sys
import oss2
from oss2 import SizedFileAdapter, determine_part_size
from oss2.models import PartInfo
import json
import time
import tarfile
import requests
from dpgen import dlog

class DPGEN():
    def __init__(self):
        self.config_json = eval(open("./config.json").read())
        # self.config_json['previous_job_id'] = -1
        self.base_url = self.config_json['host'].strip('/') + ":%s" % self.config_json['port'] + '/'
        self.endpoint = self.config_json["endpoint"]
        self.username = self.config_json["username"]
        self.password = self.config_json["password"]
        self.bucket = self.config_json["bucket"]
        self.remote_oss_url = self.endpoint[:7] + self.bucket + "." + self.endpoint[7:].strip("/")  + "/"
        self.headers = {'Content-Type': 'application/json'}
        self.cookies = ""

    def login(self):
        json_data = {"username": self.username, "password": self.password}
        for i in range(5):
            res = requests.post(self.base_url + 'login', json=json_data)
            if res.json()['result'] == 1:
                login_status = True
                cookies = requests.utils.dict_from_cookiejar(res.cookies)
                self.cookies = cookies
                break
            time.sleep(1)
        if login_status == True:
            print("welcome ", self.username, "login successfully!", time.ctime())
        else:
            print("login failed, please check network or username and password")

    def update_config(self):
        w = open('config.json', 'w')
        w.write(json.dumps(self.config_json, indent=4, ensure_ascii=False))
        w.close()

    def get_url(self, url, **kwargs):
        url = self.base_url + url
        for i in range(5):
            time.sleep(1)
            try:
                res = requests.get(url, params=kwargs, headers=self.headers, cookies=self.cookies, timeout=3)
                return res.json()
            except Exception as e:
                dlog.error("get url %s error: %s" % (url, str(e)))
        return {}

    def post_url(self, url, json_data):
        url = self.base_url + url
        for i in range(5):
            time.sleep(1)
            try:
                res = requests.post(url, json=json_data, headers=self.headers, cookies=self.cookies, timeout=10)
                return res.json()
            except Exception as e:
                dlog.error("post url %s error: %s" % (url, str(e)))
        return {}

    def get_bucket(self):
        url = "get_sts_token"
        oss_info = self.get_url(url)
        key_id = oss_info['AccessKeyId']
        key_secret = oss_info['AccessKeySecret']
        token = oss_info['SecurityToken']
        auth = oss2.StsAuth(key_id, key_secret, token)
        bucket_obj = oss2.Bucket(auth, self.endpoint, self.bucket)
        return bucket_obj

    def upload_file_to_oss(self, oss_task_dir, zip_task_file):
        bucket = self.get_bucket()
        i = 0
        while i < 5:
            try:
                total_size = os.path.getsize(zip_task_file)
                part_size = determine_part_size(total_size, preferred_size=1000 * 1024)
                upload_id = bucket.init_multipart_upload(oss_task_dir).upload_id
                parts = []
                with open(zip_task_file, 'rb') as fileobj:
                    part_number = 1
                    offset = 0
                    while offset < total_size:
                        num_to_upload = min(part_size, total_size - offset)
                        result = bucket.upload_part(oss_task_dir, upload_id, part_number, SizedFileAdapter(fileobj, num_to_upload))
                        parts.append(PartInfo(part_number, result.etag))
                        offset += num_to_upload
                        part_number += 1
                bucket.complete_multipart_upload(oss_task_dir, upload_id, parts)
                break
            except:
                i += 1

    def download_file_from_oss(self, oss_path, local_dir):
        bucket = self.get_bucket()
        local_file = oss_path.split('/')[-1]
        i = 1
        while i < 5:
            try:
                bucket.get_object_to_file(oss_path, os.path.join(local_dir, local_file))
                break
            except Exception as e:
                i += 1
        cwd = os.getcwd()
        os.chdir(local_dir)
        with tarfile.open(local_file, "r:gz") as tar:
            tar.extractall()
        os.remove(local_file)
        os.chdir(cwd)

    def submit_job(self, input_data, previous_job_id=None):
        data = {
            'job_type': "dpgen",
            'oss_path': input_data['oss_path'],
            'username': self.username,
            'password': self.password,
            'input_data': input_data
        }

        if previous_job_id:
            data['previous_job_id'] = previous_job_id
        url = 'insert_job'
        time.sleep(0.2)
        self.login()
        res = self.post_url(url, data)
        return res['data']['job_id']
