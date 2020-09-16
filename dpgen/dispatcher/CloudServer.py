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
    def __init__(self, mdata, mdata_resources, work_path, run_tasks, group_size, cloud_resources):
        self.cloud_resources = cloud_resources
        self.root_job_id = -1
        self.work_path = work_path # iter.000000/02.fp
        self.run_tasks = run_tasks # ['task.000.000000', 'task.000.000001', 'task.000.000002', 'task.000.000003']
        self.ratio_failure = mdata_resources.get('ratio_failure', 0)

    def decide_type(self):
        remote_type = "" # vasp, lammps
        if os.path.exists('relaxation.json') or os.path.exists('property.json'):
            # confirm task_type: relaxation, elastic, eos, etc...
            # relaxtion : read relaxation.json
            # others: read property.json
            # then we can make sure remote_type is vasp or lammps
            pass
        elif os.path.exists('param.json'):
            pass

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
        cwd = os.getcwd()
        # print("CloudServer:", cwd, work_path, tasks)
        dpgen_data = tail('record.dpgen', 1)
        current_iter = dpgen_data[0].split(' ')[0]
        current_stage = dpgen_data[0].split(' ')[1]
        # 0, 1, 2: make_train, run_train, post_train
        # 3, 4, 5: make_model_devi, run_model_devi, post_model_devi
        # 6, 7, 8: make_fp, run_fp, post_fp
        if current_stage == '0':
            stage = "train"
        elif current_stage == '3':
            stage = "model_devi"
        elif current_stage == '6':
            stage = "fp"

        for task in tasks:
            self.of = uuid.uuid1().hex + '.tgz'
            remote_oss_dir = 'dpgen/%s' % self.of
            input_data = {}
            if os.path.exists('previous_job_id'):
                input_data['previous_job_id'] = tail('previous_job_id', 1)[0]
            input_data['dpgen'] = True
            input_data['job_type'] = 'dpgen'
            input_data['job_resources'] = 'http://dpcloudserver.oss-cn-shenzhen.aliyuncs.com/' + remote_oss_dir
            input_data['command'] = command[0]
            input_data['backward_files'] = backward_task_files
            input_data['local_dir'] = os.path.join(work_path, task)
            input_data['task'] = task
            input_data['sub_stage'] = current_stage # 0: train, 3: model_devi, 6: fp
            input_data['username'] = self.cloud_resources['username']
            input_data['password'] = self.cloud_resources['password']
            input_data['machine'] = {}
            input_data['machine']['resources'] = self.cloud_resources
            for key, value in resources.items():
                input_data['machine']['resources'][key] = value
            # for machine config, such as kernel, GPU etc...

            if os.path.exists(os.path.join(work_path, task, 'tag_upload')):
                continue
            # [iter]:1  |  [stage]:train  |  [task]:000
            # compress files in two folders, upload to oss and touch upload_tag, then remove local tarfile
            tar_dir(self.of, forward_common_files, forward_task_files, work_path, os.path.join(work_path, task), forward_task_dereference)
            upload_file_to_oss(remote_oss_dir, self.of)
            dlog.info(" submit [iter]:{}  |  [stage]:{}  |  [task]:{}".format(current_iter, stage, task))
            os.mknod(os.path.join(work_path, task, 'tag_upload')) # avoid submit twice
            os.remove(self.of)

            # all subtask belong to one dpgen job which has one job_id, for statistic
            if not os.path.exists('previous_job_id'):
                self.previous_job_id = submit_job(input_data)
                input_data['previous_job_id'] = self.previous_job_id
                with open('previous_job_id', 'w') as fp:
                    fp.write(str(self.previous_job_id))
            else:
                previous_job_id = tail('previous_job_id', 1)[0]
                input_data['previous_job_id'] = previous_job_id
                submit_job(input_data, previous_job_id)

        while not self.all_finished(input_data, current_iter, current_stage):
            time.sleep(10)


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





