import subprocess
import datetime
import json
import sys
import os
import glob
import time 

def get_time():
    now = datetime.datetime.utcnow()
    year = str(now.year)
    month = str(now.month)
    if len(month) == 1:
        month = '0'+month
    day = str(now.day)
    return year, month, day

def remove_all_mr_logs():
    year, month, day = get_time()
    hdfspath = '/tmp/hadoop-yarn/staging/history/done/%s/%s/%s/000000/*' % (year, month, day)
    subprocess.call(['hadoop', 'dfs', '-rm', '-f', hdfspath]) 

def copy_mr_log_file(queryNum, t):

    year, month, day = get_time()
   
    print 'start sleep'
    # Sleep for 2 minutes - fuck this
    time.sleep(300)
    print 'stop sleep'     
    

    hdfspath = '/tmp/hadoop-yarn/staging/history/done/%s/%s/%s/000000/*.jhist' % (year, month, day)

    # List the jhist files in the remote directory
    dirpath = './data/%s%s/' % (queryNum, t)

    # Make directory
    if os.path.exists(dirpath):
        files = glob.glob(dirpath + '*')
        for f in files:
            os.remove(f)
    else:
        os.makedirs(dirpath)

    subprocess.call(['hadoop', 'dfs', '-copyToLocal', hdfspath, dirpath]) 
    localpath = glob.glob(dirpath +'*')
    print 'found files: %s' % localpath
    return localpath


def find_files(queryNum, t):
    dirpath = './data/%s%s/' % (queryNum, t)
    localpath = glob.glob(dirpath +'*')
    print 'found files: %s' % localpath
    return localpath


def get_task_stats(filepaths):
    tasks = {}
    countedMaps = 0
    countedReducers = 0

    for filepath in filepaths:     
        with open(filepath) as f:
            content = f.readlines()
        
        for i in range(2, len(content)):
            if content[i] == '' or content[i] == '\n':
                continue
            jsonLine = json.loads(content[i])
            taskType = jsonLine['type']
            if taskType == 'JOB_INITED':
                event = jsonLine['event']
            if taskType == 'TASK_STARTED':
                event = jsonLine['event']
                taskid = event['org.apache.hadoop.mapreduce.jobhistory.TaskStarted']['taskid']
                startTimestamp = event['org.apache.hadoop.mapreduce.jobhistory.TaskStarted']['startTime']
                task = event['org.apache.hadoop.mapreduce.jobhistory.TaskStarted']['taskType']
                tasks[taskid] = {'startTime':startTimestamp, 'taskType':task}
            if taskType == 'TASK_FINISHED':
                event = jsonLine['event']
                successful = event['org.apache.hadoop.mapreduce.jobhistory.TaskFinished']['status']
                taskid = event['org.apache.hadoop.mapreduce.jobhistory.TaskFinished']['taskid']
                if successful == "SUCCEEDED":
                  endTimestamp = event['org.apache.hadoop.mapreduce.jobhistory.TaskFinished']['finishTime']
                  task = event['org.apache.hadoop.mapreduce.jobhistory.TaskFinished']['taskType']
                  tasks[taskid]['finishTime'] = endTimestamp
                else:
                  #make sure this works
                  del tasks[taskid]


    for t in tasks:
        if tasks[t]['taskType'] == 'MAP':
            countedMaps += 1
        if tasks[t]['taskType'] == 'REDUCE':
            countedReducers += 1

    totalTasks = countedMaps + countedReducers
    if countedReducers == 0:
        ratio = 'undefined'
    else:
        ratio = float(countedMaps)/float(countedReducers)
    
    return totalTasks, ratio, tasks

if __name__ == '__main__':
    filepaths =  find_files('12', 'mr')
    results=  get_task_stats(filepaths)
    import pprint 
    pp = pprint.PrettyPrinter(indent=3)
    pp.pprint(results)
    #remove_all_mr_logs()
