import uuid
import os
import sys
import oss2
import tarfile
import requests
class CloudServer:
    def __init__(self, mdata, mdata_resource, work_path, run_tasks, group_size, cloud_resources):
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
        # 有一堆他传过来的东西
        # work_dir, tasks, group_size, forward_files, backward_files, command...
        # 需要加一个stage参数，让我知道是啥命令
        cwd = os.getcwd()
        print("CloudServer:", cwd, work_path, tasks)
        dpgen_data = tail('record.dpgen', 1)
        current_iter = dpgen_data[0].split(' ')[0]
        current_stage = dpgen_data[0].split(' ')[1]
        # 0, 1, 2: make_train, run_train, post_train
        # 3, 4, 5: make_model_devi, run_model_devi, post_model_devi
        # 6, 7, 8: make_fp, run_fp, post_fp
        print(dpgen_data, current_iter, current_stage)
        for task in tasks:
            print(task)
            self.of = uuid.uuid1().hex + '.tgz'
            tar_dir(self.of, forward_common_files, forward_task_files, work_path, os.path.join(work_path, task), forward_task_dereference)
            remote_oss_dir = 'dpgen/%s' % self.of
            upload_file_to_oss(remote_oss_dir, self.of)
            os.remove(self.of)
        sys.exit()

        if current_stage == "0":
            self.submit_train()
        elif stage == "3":
            self.submit_model_devi()
        elif stage == "6":
            self.submit_fp()


    def submit_train(self):
        tmp_uuid = uuid.uuid1().hex
        zip_dir(train_job_dir, self.work_dir + "%s.zip" % tmp_uuid)
        upload_oss()
        input_data = {}
        submit_job('train', input_data)

    def submit_model_devi(self):
        model_devi_job_dir = os.path.join(self.work_dir, '01.model_devi')
        tmp_uuid = uuid.uuid1().hex
        zip_dir(model_devi_job_dir, self.work_dir + "%s.zip" % tmp_uuid)
        upload_oss()
        input_data = {}
        submit_job('model_devi', input_data)

    def submit_fp(self):
        fp_job_dir = os.path.join(self.work_dir, '02.fp')
        tmp_uuid = uuid.uuid1().hex
        zip_dir(fp_job_dir, self.work_dir + "%s.zip" % tmp_uuid)
        upload_oss()
        input_data = {}
        submit_job('fp', input_data)

def submit_job(sub_stage, input_data, root_job_id=None):
    data = {
        'job_type': "dpgen",
        'user_name': 'dingzhaohan',
        'input_data': input_data
    }
    if root_job_id:
        data['root_job_id'] = root_job_id
    print(data)
    url = 'http://39.98.150.188:5001/insert_job'
    headers = {'Content-Type': 'application/json'} ## headers中添加上content-type这个参数，指定为json格式
    time.sleep(0.2)
    res = requests.post(url=url, headers=headers, data=json.dumps(data)) ## post的时候，将data字典形式的参数用json包转换成json格式。
    print(res.text)

def tar_dir(of, comm_files, task_files, comm_dir, task_dir,  dereference=True):
    cwd = os.getcwd()
    print(cwd)
    with tarfile.open(of, "w:gz", dereference = dereference) as tar:
        os.chdir(comm_dir)
        for ii in comm_files:
            tar.add(ii)
        os.chdir(cwd)
        os.chdir(task_dir)
        for ii in task_files:
            tar.add(ii)
    os.chdir(cwd)

#获取文件最后N行的函数
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
        #最后两行，N行就改数字，即可
        last_line = lines[-num_of_lines:]
    #print "last line : ", last_line
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
    end_point = 'http://oss-us-west-1.aliyuncs.com'
    bucket_name = "deepmp"
    bucket = oss2.Bucket(auth, end_point, bucket_name)
    print("get token successfully!")
    return bucket

def upload_file_to_oss(oss_task_dir, zip_task_file):
    bucket = get_bucket()
    total_size = os.path.getsize(zip_task_file)
    # determine_part_size方法用于确定分片大小。
    part_size = determine_part_size(total_size, preferred_size=1000 * 1024)
    upload_id = bucket.init_multipart_upload(oss_task_dir).upload_id
    parts = []
    with open(zip_task_file, 'rb') as fileobj:
        part_number = 1
        offset = 0
        while offset < total_size:
            num_to_upload = min(part_size, total_size - offset)
            # 调用SizedFileAdapter(fileobj, size)方法会生成一个新的文件对象，重新计算起始追加位置。
            result = bucket.upload_part(oss_task_dir, upload_id, part_number, SizedFileAdapter(fileobj, num_to_upload))
            parts.append(PartInfo(part_number, result.etag))
            offset += num_to_upload
            part_number += 1
    bucket.complete_multipart_upload(oss_task_dir, upload_id, parts)

