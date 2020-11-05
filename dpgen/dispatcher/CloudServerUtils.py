
import requests, json, tarfile, zipfile, time, oss2, os, sys
from oss2 import SizedFileAdapter, determine_part_size
from oss2.models import PartInfo
from dpgen import dlog
class DPLibray(object):
    def __init__(self, jdata, current_iter, current_stage):
        self.jdata = jdata
        self.current_iter = int(current_iter)
        self.current_stage = current_stage

    def get_iterations(self):
        result = ''
        tmp_data = []
        try:
            tmp_result = os.popen('wc -l iter.%.06d/02.fp/*.out | grep out' % self.current_iter)
            tmp_result = tmp_result.read()
            tmp_data = [[int(line.strip().split(' ')[0]), self.get_sys(line.strip().split(' ')[1]), int(line.split('.')[-2])] for line in tmp_result.strip().split('\n') if line]
        except:
            tmp_result = ''
        if 'out' in tmp_result:
            result += tmp_result
        strip = int(len(tmp_data) / 3)
        candidate = tmp_data[:strip]
        rest_accurate = tmp_data[strip:strip*2]
        rest_failed = tmp_data[strip*2:]
        data_list = []
        tmp_dic = {}
        for ii in range(len(candidate)):
            if candidate[ii][1] in tmp_dic:
                tmp_dic[candidate[ii][1]]["candidate"] += candidate[ii][0]
                tmp_dic[candidate[ii][1]]["rest_accurate"] += rest_accurate[ii][0]
                tmp_dic[candidate[ii][1]]["rest_failed"] += rest_failed[ii][0]
                tmp_dic[candidate[ii][1]]["energy_raw"] += self.get_work_energy(candidate[ii][2])
            else:
                tmp_dic[candidate[ii][1]] = {}
                tmp_dic[candidate[ii][1]]["candidate"] = candidate[ii][0]
                tmp_dic[candidate[ii][1]]["rest_accurate"] = rest_accurate[ii][0]
                tmp_dic[candidate[ii][1]]["rest_failed"] = rest_failed[ii][0]
                tmp_dic[candidate[ii][1]]["energy_raw"] = self.get_work_energy(candidate[ii][2])
        result = []
        for key, value in tmp_dic.items():
            tmp_dic = {}
            tmp_dic['sys_configs'] = key
            for key1, value1 in value.items():
                tmp_dic[key1] = value1
            result.append(tmp_dic)
        return result

    def get_sys(self, line):
        sys_config_idx = int(line.strip().split('/')[-1].split('.')[-2])
        tmp_data = self.jdata['sys_configs'][sys_config_idx][0].strip()
        tmp_data_0 = tmp_data.split('/')
        for ii in range(3):
            if '01x01x01' in tmp_data_0[ii] or '02x02x02' in tmp_data_0[ii]:
                return '/'.join(tmp_data_0[0:ii+1])
        return ""

    def get_work_energy(self, sys_config_idx):
        result = os.popen('wc -l  iter.%.06d/02.fp/data.%.03d/ener* |grep energy' % (self.current_iter, sys_config_idx))
        result = int(result.read().strip().split(' ')[0])
        return result


    def get_sys_configs(self):
        param_json = self.jdata
        max_iter = max(list(x['_idx'] for x in param_json['model_devi_jobs']))
        sys_configs = param_json['sys_configs']
        sys_info_dict = {}
        count_0 = 0
        for tmp_sys_info in sys_configs:
            tmp_sys_info = tmp_sys_info[0].split('/')[-6].split('.')
            if len(tmp_sys_info) == 1:
                tmp_sys_info = tmp_sys_info[0]
            else:
                tmp_sys_info = tmp_sys_info[1]
            count_0 += 1
        sys_info_dict['max_iter'] = max_iter
        return sys_info_dict

    def get_lcurve_out(self):
        all_file = os.popen("ls iter.%.06d/00.train/00[0-3]/lcurve.out|grep -v 'No such file'" % self.current_iter)
        all_file = all_file.read()
        all_file = all_file.strip().split('\n')
        data_list = []
        for line in all_file:
            data = open(line).read()
            tmp_dic = {}
            tmp_dic["dir_path"] = line.strip().split('/')[-2]
            tmp_dic["data"] = data
            data_list.append(tmp_dic)
        return data_list



class Auth(object):
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
        self.rerun = self.config_json.get("rerun", 0)

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

    def rerun_dpgen(self, last_job_id, new_job_id, current_iter):
        url = "rerun_dpgen"
        data = {
            "last_job_id": last_job_id,
            "new_job_id": new_job_id,
            "iter": current_iter
        }
        res = self.post_url(url, data)


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
        url = 'insert_job'
        time.sleep(0.2)
        self.login()
        if previous_job_id:
            if self.rerun:
                res = self.post_url(url, data)
                new_job_id = res['data']['job_id']
                self.rerun_dpgen(previous_job_id, new_job_id, input_data['current_iter'])
                print(previous_job_id, new_job_id, input_data['current_iter'])
                self.rerun = 0
                self.config_json['rerun'] = 0
                self.update_config()
            else:
                data['previous_job_id'] = previous_job_id
                res = self.post_url(url, data)
        else:
            res = self.post_url(url, data)
        return res['data']['job_id']

class Utils(object):
    def __init__(self):
        pass

    def dinfo_upload(self, job_info):
        if job_info['type'] == 'run':
            dlog.info(" submit [stage]:{}  |  [task]:{}".format(job_info['stage'], job_info['task']))
        elif job_info['type'] == 'autotest':
            pass

    def dinfo_download(self, input_data):
        pass


def tar_dir(of, comm_files, task_files, comm_dir, tasks):
    cwd = os.getcwd()
    print(task_files)
    task_files.append('task.json')
    print("tar_dir, cwd", cwd)
    print("work_dir", comm_dir)
    with tarfile.open(of, "w:gz") as tar:
        os.chdir(os.path.join(cwd, comm_dir))
        for ii in comm_files:
            tar.add(ii)
        for task in tasks:
            with open(os.path.join(task, 'task.json'), 'w') as fp:
                fp.write(task)
            for ii in task_files:
                tar.add(os.path.join(task, ii))
    os.chdir(cwd)

def tail(inputfile, num_of_lines):
    filesize = os.path.getsize(inputfile)
    blocksize = 1024
    dat_file = open(inputfile, 'r')
    last_line = ""
    if filesize > blocksize :
        maxseekpoint = (filesize // blocksize)
        dat_file.seek((maxseekpoint-1)*blocksize)
    elif filesize :
        dat_file.seek(0, 0)
    lines =  dat_file.readlines()
    if lines :
        last_line = lines[-num_of_lines:]
    dat_file.close()
    data = []
    for line in last_line:
        data.append(line.replace('\n', ''))
    return data
