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

class CloudServer:
    def __init__(self, mdata, mdata_resources, work_path, run_tasks, group_size, cloud_resources, ttype="run"):
	# type: run, autotest, simplily, init_surf, init_bulk...
        self.type = ttype
        self.cloud_resources = cloud_resources
        self.root_job_id = -1
        self.work_path = work_path # iter.000000/02.fp
        self.run_tasks = run_tasks # ['task.000.000000', 'task.000.000001', 'task.000.000002', 'task.000.000003']
        self.ratio_failure = mdata_resources.get('ratio_failure', 0)

    def run_jobs(self,
            resources,
            command,
            work_path,
            tasks,
            group_size,
            forward_common_files,
            forward_task_files,
            backward_task_files,
            forward_task_dereference = True,
            mark_failure = False,
            outlog = 'log',
            errlog = 'err'):
        job_info = {}
        if self.type == 'run':
            job_info = self.get_run_info()
        elif self.type == 'autotest':
            self.get_autotest_info()
        elif self.type == 'init_surf':
            pass
        elif self.type == 'init_bulk':
            pass
        elif self.type == 'simplify':
            pass

        job_info["resources"] = resources
        job_info["command"] = command
        job_info["work_path"] = work_path
        job_info["tasks"] = tasks
        job_info["forward_common_files"] = forward_common_files
        job_info["forward_task_files"] = forward_task_files
        job_info["backward_task_files"] = backward_task_files
        job_info["forward_task_dereference"] = forward_task_dereference
        input_data = self.tar_upload_submit_tasks(job_info)

        while not self.all_finished(input_data, job_info['current_iter'], job_info['current_stage']):
            time.sleep(10)

    def get_run_info(self):
        run_data = tail('record.dpgen', 1)
        current_iter = run_data[0].split(' ')[0]
        current_stage = run_data[0].split(' ')[1]
        # 0, 1, 2: make_train, run_train, post_train
        # 3, 4, 5: make_model_devi, run_model_devi, post_model_devi
        # 6, 7, 8: make_fp, run_fp, post_fp
        stage = ""
        if current_stage == '0':
            stage = "train"
        elif current_stage == '3':
            stage = "model_devi"
        elif current_stage == '6':
            stage = "fp"
        job_info = {
	    "type": "run",
	    "current_iter": current_iter,
	    "current_stage": current_stage,
            "stage": stage,
            "max_iter": 100
        }
        return job_info

    def get_autotest_info(self):
        job_info = {
            "type": "autotest",
            "current_stage": "elastic", # relaxation, eos, etc...
            "sub_type": "lammps"
        }
        return job_info

    def get_initsurf_info(self):
        job_info = {
            "type": "initsurf",
            "sub_type": "fp"
        }
        pass

    def get_initbulk_info(self):
        job_info = {
            "type": "initbulk",
            "sub_type": "lammps"
        }
        pass

    def get_simplify_info(self):
        job_info = {
            "type": "simplify"
        }
        pass

    def tar_upload_submit_tasks(self, job_info):
        input_data = {}
        for task in job_info["tasks"]:
            self.of = uuid.uuid1().hex + '.tgz'
            remote_oss_dir = '{}/{}'.format(job_info['type'], self.of)
            if os.path.exists('previous_job_id'):
                input_data['previous_job_id'] = tail('previous_job_id', 1)[0]
            work_path = job_info['work_path']
            input_data['dpgen'] = True
            input_data['job_type'] = 'dpgen'
            input_data['job_resources'] = 'http://dpcloudserver.oss-cn-shenzhen.aliyuncs.com/' + remote_oss_dir
            input_data['command'] = job_info["command"][0]
            input_data['backward_files'] = job_info["backward_task_files"]
            input_data['local_dir'] = os.path.join(work_path, task)
            input_data['task'] = task
            input_data['sub_stage'] = job_info["current_stage"] # 0: train, 3: model_devi, 6: fp
            input_data['username'] = self.cloud_resources['username']
            input_data['password'] = self.cloud_resources['password']
            input_data['machine'] = {}
            input_data['machine']['resources'] = self.cloud_resources
            for key, value in job_info["resources"].items():
                input_data['machine']['resources'][key] = value
            # for machine config, such as kernel, GPU etc...

            if os.path.exists(os.path.join(work_path, task, 'tag_upload')):
                continue

            # compress files in two folders, upload to oss and touch upload_tag, then remove local tarfile
            tar_dir(self.of, job_info['forward_common_files'], job_info['forward_task_files'], work_path, os.path.join(work_path, task), job_info["forward_task_dereference"])
            upload_file_to_oss(remote_oss_dir, self.of)
            # TODO: finish dlog info
            # dlog.info(" submit [stage]:{}  |  [task]:{}".format(stage, task))
            os.mknod(os.path.join(work_path, task, 'tag_upload')) # avoid submit twice
            os.remove(self.of)
            print(os.getcwd())
            # all subtask belong to one dpgen job which has one job_id, for statistic
            if not os.path.exists('previous_job_id'):
                self.previous_job_id = submit_job(input_data)
                # print(1, self.previous_job_id)
                input_data['previous_job_id'] = self.previous_job_id
                with open('previous_job_id', 'w') as fp:
                    fp.write(str(self.previous_job_id))
            else:
                previous_job_id = tail('previous_job_id', 1)[0]
                # print(2, previous_job_id)
                input_data['previous_job_id'] = previous_job_id
                submit_job(input_data, previous_job_id)
        return input_data

    # 获取所有的jobs directory
    def get_iterations(self, path):
        result = ''
        if path[-1] == '/' or path[-1] == '\\':
            path = path[:-1]
        for tmp_iter in os.popen('ls  %s/|grep "iter."' % path).read().split('\n'):
            if not tmp_iter.split('.')[-1].isdigit():
                continue
            try:
                tmp_result = os.popen('wc -l  %s/%s/02.fp/*.out|grep out' % (path, tmp_iter))
                tmp_result = tmp_result.read()
            except:
                tmp_result = ''
            if 'out' in tmp_result:
                result += tmp_result
        return result

    # 获取 work energy 信息
    def get_work_energy(self, path):
        if path[-1] == '/' or path[-1] == '\\':
            path = path[:-1]
        result = os.popen('wc -l  %s/iter.*/02.fp/data*/ener* |grep energy' % path)
        result = result.read().strip()
        return result

     # 获取构型数据 和 最大迭代数据
    def get_sys_configs(self, param_file='param.json'):
        param_json = json.loads(open(param_file).read() )
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

     # 上传 构型和 iterations 信息
    def post_iter_sys_configs(self, path, param_file='param.json'):
        iterations_data = self.get_iterations(path=path)
        energy_data = self.get_work_energy(path=path)
        sys_configs = self.get_sys_configs(param_file)
        json_data = {'iterations_data':iterations_data, 'sys_configs':sys_configs, 'energy_data':      energy_data}
        return self.post_data(url='insert_iter_data', json_data=json_data)

     # 获取lcurve out 误差信息
    def post_lcurve_out(self, path):
        #TODO 需要获取当前的lcurve 误差信息，可以用来减少误差上传量
        if path[-1] == '/' or path[-1] == '\\':
            path = path[:-1]
        for tmp_iter in os.popen('ls  %s/|grep "iter."' % path).read().split('\n'):
            if not tmp_iter.split('.')[-1].isdigit():
                continue
            all_file = os.popen("ls -l -t --time-style='+%s'" + " %s/%s/00.train/00[0-3]/lcurve.out|grep -v 'No such file'" % (path, tmp_iter))
            all_file = all_file.read()
            if all_file == '':
                continue
            all_file = all_file.strip().split('\n')
            for line in all_file:
                time_0, path_doc = line.split(' ')[-2:]
                crawl_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                data_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time_0)))
                data = open(path_doc).read()
                json_data = {'path':path_doc, 'lcurve_data':data}
                result = self.post_data(url='insert_lcurve_out', json_data=json_data)
                if result['result'] == 'error':
                    print(path_doc)

    def all_finished(self, input_data, current_iter, current_stage):
        analyse_and_download(input_data, current_iter, current_stage)
        finish_num = 0
        for ii in self.run_tasks:
            if os.path.exists(os.path.join(self.work_path, ii, 'tag_download')):
                finish_num += 1
        if finish_num / len(self.run_tasks) < (1 - self.ratio_failure): return False
        return True


def analyse_and_download(input_data, current_iter, current_stage):
    return_data = get_job_summary(input_data)
    current_iter = int(current_iter)
    current_data = [ii for ii in return_data if ii['iter'] == current_iter and ii['sub_stage'] == current_stage]
    if current_stage == '0':
        stage = "train"
    elif current_stage == '3':
        stage = "model_devi"
    elif current_stage == '6':
        stage = "fp"
    for ii in current_data:
        if os.path.exists(os.path.join(ii['local_dir'], "tag_download")):
            continue
        else:
            if ii['status'] == 2:
                download_file_from_oss(ii['result'], ii['local_dir'])
                dlog.info(" download [iter]:{}  |  [stage]:{}  |  [task]:{}".format(current_iter, stage, ii["task"]))
                os.mknod(os.path.join(ii['local_dir'], 'tag_download'))


def get_job_summary(input_data):
    url = 'http://39.98.150.188:5001/get_job_details?job_id=%s&username=%s' % (input_data['previous_job_id'], input_data['username'])
    headers = {'Content-Type': 'application/json'}
    time.sleep(0.2)
    return_data = []
    i = 0
    while i < 3:
        try:
            res = requests.get(url=url, headers=headers)
            data = res.json()['data']
            details = res.json()['details']
            for ii in details:
                res_input_data = eval(ii['input_data'])
                tmp_data = {}
                tmp_data['sub_stage'] = res_input_data["sub_stage"]                        # '0', '3', '6'
                tmp_data["local_dir"] = res_input_data["local_dir"]                        # 'iter.000000/01.model_devi/task.000.000009'
                tmp_data["job_type"] = res_input_data["job_type"]                          # 'kit', 'lammps', 'fp'
                tmp_data["task"] = res_input_data["task"]
                tmp_data["status"] = ii["status"]                                          # 0, 1, 2 | unsubmitted, running, finished
                tmp_data["result"] = ii["result"]                                          # oss_download_addr: "dpgen/699c2b26ed1011ea95c7a5aeac438cd3.download.tgz"
                tmp_data["task_id"] = ii["task_id"]
                tmp_data["iter"] = int(tmp_data["local_dir"].split("/")[0].split('.')[1])  # iter.000001 -> 1
                return_data.append(tmp_data)
            break
        except Exception as e:
            i += 1
    return return_data


def submit_job(input_data, previous_job_id=None):
    data = {
        'job_type': "dpgen",
        'username': input_data['username'],
        'password': input_data['password'],
        'input_data': input_data
    }
    if previous_job_id:
        data['previous_job_id'] = previous_job_id
    url = 'http://39.98.150.188:5001/insert_job'
    headers = {'Content-Type': 'application/json'}
    time.sleep(0.2)
    res = requests.post(url=url, headers=headers, data=json.dumps(data))

    return res.json()['job_id']

def tar_dir(of, comm_files, task_files, comm_dir, task_dir,  dereference=True):
    cwd = os.getcwd()
    # print(cwd)
    with tarfile.open(of, "w:gz", dereference = dereference) as tar:
        os.chdir(comm_dir)
        for ii in comm_files:
            tar.add(ii)
        os.chdir(cwd)
        os.chdir(task_dir)
        for ii in task_files:
            tar.add(ii)
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
        #maxseekpoint = blocksize % filesize
        dat_file.seek(0, 0)
    lines =  dat_file.readlines()
    if lines :
        #last_line = lines[-1].strip()
        last_line = lines[-num_of_lines:]
    dat_file.close()
    data = []
    for line in last_line:
        data.append(line.replace('\n', ''))
    return data

def get_bucket():
    url_0 = "http://39.98.150.188:5005/"  + "get_sts_token"
    i = 0
    while i < 5:
        try:
            res = requests.get(url_0)
            break
        except Exception as e:
            i += 1
            print(e)
    oss_info = res.json()['data']
    key_id = oss_info['AccessKeyId']
    key_secret = oss_info['AccessKeySecret']
    token = oss_info['SecurityToken']
    auth = oss2.StsAuth(key_id, key_secret, token)
    end_point = 'http://oss-cn-shenzhen.aliyuncs.com'
    bucket_name = "dpcloudserver"
    bucket = oss2.Bucket(auth, end_point, bucket_name)
    return bucket

def upload_file_to_oss(oss_task_dir, zip_task_file):
    bucket = get_bucket()
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

def download_file_from_oss(oss_path, local_dir):
    bucket = get_bucket()
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





