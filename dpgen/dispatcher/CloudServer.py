
import requests
import json
import zipfile
import os
import oss2
def zipDir(dirpath,outFullName):
    """
    压缩指定文件夹
    :param dirpath: 目标文件夹路径
    :param outFullName: 压缩文件保存路径+xxxx.zip
    :return: 无
    """
    zip = zipfile.ZipFile(outFullName,"w",zipfile.ZIP_DEFLATED)
    for path,dirnames,filenames in os.walk(dirpath):
        # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
        fpath = path.replace(dirpath,'')
        for filename in filenames:
            zip.write(os.path.join(path,filename),os.path.join(fpath,filename))
    zip.close()

class CloudServer():
    def __init__(self, mdata_machine, mdata_resources, work_path, run_tasks, group_size, cloud_resources=None):
        self.mdata_machine = mdata_machine
        self.mdata_resources = mdata_resources
        self.work_path = work_path
        auth = oss2.Auth("LTAI4FwshjFwCTD7tFHmQfh1", "Oea8lGGbpNM1Bw8UmSMu2Deft8ve7e")
        self.bucket = oss2.Bucket(auth, 'http://oss-cn-shenzhen.aliyuncs.com', "dpgen-oss-test")

    def run_jobs(self,
                 resources,
                 command,
                 work_path,
                 tasks,
                 group_size,
                 forward_common_files,
                 forward_task_files,
                 backward_task_files,
                 forward_task_deference = True,
                 mark_failure = False,
                 outlog = 'log',
                 errlog = 'err'):
        # work_path = os.path.join("_perturbations", "gromacs")
        # run_tasks = glob.glob("complex/") + glob.glob("solvated/")
        work_path = '/root/DP-Jobfile/fep'
        cwd = os.getcwd()
        os.chdir(work_path)
        outFullName = work_path + 'job.zip'
        zipDir(work_path, outFullName)
        
        with open(outFullName, 'rb') as fileobj:
            oss_task_dir = 'fep'
            self.bucket.put_object(oss_task_dir, fileobj)
        
        frontend_data = {
            "user_id":       "000",
            "job_id":        "000",
            "job_type":      "fp",
            "job_resources": "https://dpgen-oss-test.oss-cn-shenzhen.aliyuncs.com/DP-CloudServer/000/fep/000/job.zip",
            "_comment":      "manager: download job.zip, unzip it, and assign tasks to machine, then zip task, upload to oss",
            "_comment":      "https://dpgen-oss-test.oss-cn-shenzhen.aliyuncs.com/DP-CloudServer/000/fep/000/complex.zip",
            "_comment":      "https://dpgen-oss-test.oss-cn-shenzhen.aliyuncs.com/DP-CloudServer/000/fep/000/solvated.zip",
            "_comment":      "machine: download task.zip, unzip, when finishing, zip result, upload to oss",
            "_comment":      "https://dpgen-oss-test.oss-cn-shenzhen.aliyuncs.com/DP-CloudServer/000/fep/000/complex.back.zip",
            "_comment":      "https://dpgen-oss-test.oss-cn-shenzhen.aliyuncs.com/DP-CloudServer/000/fep/000/solvated.back.zip",
            "backward_files": ["_sol.tpr", "_sol.log", "dhdl.xvg", "gmx.log"],
            "log_file": "gmx.log",
            "machine": {
                "platform": "ali",
                "resources": {
                    "gpu_num":      0,
                    "cpu_num":      16,
                    "mem_limit":    30,
                    "time_limit":   "23:00:00",
                    "docker_name":  "vasp",           
                    "type":         "ecs.c6.4xlarge",      
                    "region":       "cn-huhehaote",         
                    "zone":         "cn-huhehaote-a",      
                    "image_name":   "vasp"             
                }
            }
        }
        data = {
            'job_type':'fep',
            'input_data': frontend_data
        }
        print(data)

        url = 'http://39.98.150.188:5001/insert_job'
        headers = {'Content-Type': 'application/json'} ## headers中添加上content-type这个参数，指定为json格式
        time.sleep(0.2)
        res = requests.post(url=url, headers=headers, data=json.dumps(data)) ##   