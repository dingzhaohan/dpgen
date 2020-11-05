import uuid
import os
import json
import time
from dpgen import dlog
from dpgen.dispatcher.CloudServerUtils import Auth, DPLibray, Utils, tar_dir, tail
from dpgen.dispatcher.Dispatcher import _split_tasks

class CloudServer:
    def __init__(self, mdata, mdata_resources, work_path, run_tasks, group_size, cloud_resources, jdata=None, ttype="run"):
	# type: run, autotest, simplily, init_surf, init_bulk...
        self.dpgen = Auth()
        self.jdata = jdata
        self.type = ttype
        self.cloud_resources = cloud_resources
        self.root_job_id = -1
        self.work_path = work_path # iter.000000/02.fp
        self.run_tasks = run_tasks # ['task.000.000000', 'task.000.000001', 'task.000.000002', 'task.000.000003']
        self.group_size = group_size
        self.task_chunks = _split_tasks(run_tasks, group_size)
        self.ratio_failure = mdata_resources.get('ratio_failure', 0)
        # TODO
        # another problem: we hope calculate all vasp task instead of finish calculation when address ratio_failure.
        # previous_job_id, current_job_id ?
        print(self.ratio_failure)

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

        input_data = self.tar_upload_submit_tasks(resources, command, work_path, tasks, forward_common_files, forward_task_files, backward_task_files, forward_task_dereference, job_info)

        while not self.all_finished(input_data):
            time.sleep(10)

        self.upload_summary_data(job_info)

    # upload lcurve.out, sys_config to dp_lib
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
        current_iter = int(run_data[0].split(' ')[0])
        current_stage = int(run_data[0].split(' ')[1])
        # 0, 1, 2: make_train, run_train, post_train
        # 3, 4, 5: make_model_devi, run_model_devi, post_model_devi
        # 6, 7, 8: make_fp, run_fp, post_fp
        stage = ""
        if current_stage == 0:
            stage = "train"
        elif current_stage == 3:
            stage = "model_devi"
        elif current_stage == 6:
            stage = "fp"
        job_info = {
	    "type": "run",
	    "current_iter": current_iter,
	    "current_stage": current_stage,
            "stage": stage,
            "max_iter": max(list(x['_idx'] for x in self.jdata['model_devi_jobs']))
        }
        return job_info

    # uuid.tgz
    # |--G_uuid.tgz
    #      |--T_uuid.tgz // files can run straight.
    #      |--T_uuid.tgz
    # |--G_uuid.tgz
    # |--G_uuid.tgz
    def tar_upload_submit_tasks(self, resources, command, work_path, tasks, forward_common_files, forward_task_files, backward_task_files, forward_task_dereference, job_info):
        input_data = {}
        input_data['type'] = job_info['type']
        input_data['tasks'] = tasks
        # submit batch jobs
        # TODO: how to download?
        # tar task
        # just put all task to a tarfile
        self.of = uuid.uuid1().hex + '.tgz'
        remote_oss_dir = '{}/{}'.format(job_info['type'], self.of)
        tar_dir(self.of, forward_common_files, forward_task_files, work_path, tasks)
        input_data['dpgen'] = True
        input_data['job_type'] = 'dpgen'
        input_data['job_resources'] = self.dpgen.remote_oss_url + remote_oss_dir
        input_data['oss_path'] = input_data['job_resources']
        input_data['command'] = command[0]
        input_data['forward_common_files'] = forward_common_files
        input_data['forward_task_files'] = forward_task_files
        input_data['backward_files'] = backward_task_files
        input_data['current_iter'] = job_info['current_iter']
        input_data['current_stage'] = job_info['current_iter']
        input_data['sub_stage'] = job_info["current_stage"] # 0: train, 3: model_devi, 6: fp
        input_data['username'] = self.cloud_resources['username']
        input_data['password'] = self.cloud_resources['password']
        input_data['machine'] = {}
        input_data['machine']['resources'] = self.cloud_resources
        for key, value in resources.items():
            input_data['machine']['resources'][key] = value
        # for machine config, such as kernel, GPU etc...

        # compress files in two folders, upload to oss and touch upload_tag, then remove local tarfile

        self.dpgen.upload_file_to_oss(remote_oss_dir, self.of)
        dlog.info("upload!")
        os.mknod(os.path.join(work_path, 'tag_upload')) # avoid submit twice
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

    def all_finished(self, input_data):
        self.dpgen.login()
        self.analyse_and_download(input_data)
        finish_num = 0
        print(finish_num, len(self.run_tasks),  finish_num / len(self.run_tasks))
        if finish_num / len(self.run_tasks) < (1 - self.ratio_failure): return False
        return True

    def analyse_and_download(self, input_data):
        current_iter = input_data['current_iter']
        current_stage = input_data['current_stage']
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
        for ii in range(1, int(all_task/10)+1):
            url_1 = url + '&page=%s' % ii
            res = self.dpgen.get_url(url_1)
            while not res:
                res = self.dpgen.get_url(url_1)
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


if __name__ == '__main__':

    mdata = {}
    mdata_resources = {}
    work_path = {}
    run_tasks = []
    group_size = 1
    cloud_resources = {}

    CS = CloudServer(mdata, mdata_resources, work_path, run_tasks, group_size, cloud_resources, jdata=None, ttype="run")
