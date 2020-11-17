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
from dpgen.dispatcher.Auth import DPGEN

class CloudServer:
    def __init__(self, mdata, mdata_resources, work_path, run_tasks, group_size, cloud_resources, jdata=None, ttype="run"):
	# type: run, autotest, simplily, init_surf, init_bulk...
        self.dpgen = DPGEN()
        self.jdata = jdata
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

        self.dpgen.login()
        input_data = self.tar_upload_submit_tasks(resources, command, work_path, tasks, forward_common_files, forward_task_files, backward_task_files, forward_task_dereference, job_info)

        while not self.all_finished(input_data, job_info['current_iter'], job_info['current_stage']):
            time.sleep(10)

        self.upload_summary_data(job_info)


    def upload_summary_data(self, job_info):
        self.dpgen.login()
        if job_info['type'] == 'run':
            if job_info['current_stage'] == '0':
                result = self.get_lcurve_out(job_info['current_iter'])
                for ii in result:
                    data = {
                        "job_id": self.dpgen.config_json["previous_job_id"],
                        "element": self.jdata.get("chemical_formula", "unknown"),
                        "iter": job_info["current_iter"],
                        "dir_path": ii['dir_path'],
                        "lcurve_out": ii['data'],
                        "force": 1
                    }
                    url = 'insert_lcurve_data'
                    res = self.dpgen.post_url(url, data)
            if int(job_info['current_iter']) > 0 and job_info['current_stage'] == '0':
                try:
                    result = self.get_iterations(int(job_info['current_iter']) - 1)
                    data = {
                        "job_id": self.dpgen.config_json["previous_job_id"],
                        "element": self.jdata.get("chemical_formula", "unknown"),
                        "iter": str(int(job_info["current_iter"]) - 1),
                        "data_list": result,
                        "force": 1
                    }
                    url = 'insert_iter_data'
                    res = self.dpgen.post_url(url, data)
                except Exception as e:
                    pass

        elif task_type == 'autotest':
            pass

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
            "max_iter": max(list(x['_idx'] for x in self.jdata['model_devi_jobs']))
        }
        return job_info

    def get_autotest_info(self):
        job_info = {
            "type": "autotest",
            "stage": ""
        }

    def tar_upload_submit_tasks(self, resources, command, work_path, tasks, forward_common_files, forward_task_files, backward_task_files, forward_task_dereference, job_info):
        input_data = {}
        input_data['type'] = job_info['type']
        for task in tasks:
            self.of = uuid.uuid1().hex + '.tgz'
            remote_oss_dir = '{}/{}'.format(job_info['type'], self.of)
            input_data['dpgen'] = True
            input_data['job_type'] = 'dpgen'
            input_data['job_resources'] = self.dpgen.remote_oss_url + remote_oss_dir
            input_data['oss_path'] = input_data['job_resources']
            input_data['command'] = command[0]
            input_data['backward_files'] = backward_task_files
            input_data['local_dir'] = os.path.join(work_path, task)
            input_data['task'] = task
            input_data['current_iter'] = job_info.get('current_iter', "")
            input_data['sub_stage'] = job_info.get("current_stage", "") # 0: train, 3: model_devi, 6: fp
            input_data['username'] = self.dpgen.config_json['username']
            input_data['password'] = self.dpgen.config_json['password']
            input_data['machine'] = {}
            input_data['machine']['resources'] = self.cloud_resources
            for key, value in resources.items():
                input_data['machine']['resources'][key] = value
            # for machine config, such as kernel, GPU etc...
            job_info['task'] = task

            if os.path.exists(os.path.join(work_path, task, 'tag_upload')):
                continue

            # compress files in two folders, upload to oss and touch upload_tag, then remove local tarfile
            tar_dir(self.of, forward_common_files, forward_task_files, work_path, os.path.join(work_path, task), forward_task_dereference)
            self.dpgen.upload_file_to_oss(remote_oss_dir, self.of)
            self.dinfo_upload(job_info)
            os.mknod(os.path.join(work_path, task, 'tag_upload')) # avoid submit twice
            os.remove(self.of)
            # all subtask belong to one dpgen job which has one job_id, for statistic
            if not self.dpgen.config_json['previous_job_id']:
                self.dpgen.config_json['previous_job_id'] = self.dpgen.submit_job(input_data)
                self.dpgen.update_config()
            else:
                # contain rerun condition.
                previous_job_id = self.dpgen.config_json['previous_job_id']
                self.dpgen.config_json['previous_job_id'] = self.dpgen.submit_job(input_data, previous_job_id)
                self.dpgen.update_config()
        return input_data

    def dinfo_upload(self, job_info):
        if job_info['type'] == 'run':
            dlog.info(" submit [stage]:{}  |  [task]:{}".format(job_info['stage'], job_info['task']))
        elif job_info['type'] == 'autotest':
            pass

    def dinfo_download(self, input_data):
        pass

    def get_iterations(self, current_iter):
        result = ''
        tmp_data = []
        try:
            tmp_result = os.popen('wc -l iter.%.06d/02.fp/*.out | grep out' % current_iter)
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
                tmp_dic[candidate[ii][1]]["energy_raw"] += self.get_work_energy(current_iter, candidate[ii][2])
            else:
                tmp_dic[candidate[ii][1]] = {}
                tmp_dic[candidate[ii][1]]["candidate"] = candidate[ii][0]
                tmp_dic[candidate[ii][1]]["rest_accurate"] = rest_accurate[ii][0]
                tmp_dic[candidate[ii][1]]["rest_failed"] = rest_failed[ii][0]
                tmp_dic[candidate[ii][1]]["energy_raw"] = self.get_work_energy(current_iter, candidate[ii][2])
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

    def get_work_energy(self, current_iter, sys_config_idx):
        result = os.popen('wc -l  iter.%.06d/02.fp/data.%.03d/ener* |grep energy' % (int(current_iter), sys_config_idx))
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

    def get_lcurve_out(self, current_iter):
        all_file = os.popen("ls iter.%.06d/00.train/00[0-3]/lcurve.out|grep -v 'No such file'" % int(current_iter))
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

    def all_finished(self, input_data, current_iter, current_stage):
        self.analyse_and_download(input_data, current_iter, current_stage)
        finish_num = 0
        for ii in self.run_tasks:
            if os.path.exists(os.path.join(self.work_path, ii, 'tag_download')):
                finish_num += 1
        if finish_num / len(self.run_tasks) < (1 - self.ratio_failure): return False
        return True


    def analyse_and_download(self, input_data, current_iter, current_stage):
        return_data = self.get_job_summary(input_data)
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
                    self.dpgen.download_file_from_oss(ii['result'], ii['local_dir'])
                    dlog.info(" download [iter]:{}  |  [stage]:{}  |  [task]:{}".format(current_iter, stage, ii["task"]))
                    os.mknod(os.path.join(ii['local_dir'], 'tag_download'))


    def get_job_summary(self, input_data):
        url = 'get_job_details?job_id=%s&username=%s' % (self.dpgen.config_json['previous_job_id'], input_data['username'])
        time.sleep(0.2)
        return_data = []
        res = self.dpgen.get_url(url)
        data = res['data']
        all_task = data['all_task']
        details = []
        for ii in range(1, int(all_task/10)+2):
            url_1 = url + '&page=%s' % ii
            res = self.dpgen.get_url(url_1)
            while not res.get('details', ''):
                res = self.dpgen.get_url(url_1)
                time.sleep(0.5)
            details += res['details']
        for ii in details:
            res_input_data = ii['input_data']
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
        return return_data

def tar_dir(of, comm_files, task_files, comm_dir, task_dir,  dereference=True):
    cwd = os.getcwd()
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
        dat_file.seek(0, 0)
    lines =  dat_file.readlines()
    if lines :
        last_line = lines[-num_of_lines:]
    dat_file.close()
    data = []
    for line in last_line:
        data.append(line.replace('\n', ''))
    return data






if __name__ == '__main__':

    mdata = {}
    mdata_resources = {}
    work_path = {}
    run_tasks = []
    group_size = 1
    cloud_resources = {}

    CS = CloudServer(mdata, mdata_resources, work_path, run_tasks, group_size, cloud_resources, jdata=None, ttype="run")
