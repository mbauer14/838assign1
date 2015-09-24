import json
import subprocess
import os 
import glob

def remove_all_tez_logs():
    hdfspath = '/tmp/tez-history/*'
    subprocess.call(['hadoop', 'dfs', '-rm', '-f', hdfspath]) 

def copy_tez_log_file(queryNum, t):
    hdfspath = '/tmp/tez-history/history.txt*'
    dirpath = './data/%s%s/' % (queryNum, t)
    # Make directory
    if os.path.exists(dirpath):
        files = glob.glob(dirpath + '*')
        for f in files:
            os.remove(f)
    else:
        os.makedirs(dirpath)
    # copy to direcotyr

    subprocess.call(['hadoop', 'dfs', '-copyToLocal', hdfspath, dirpath]) 
    localpath = glob.glob(dirpath +'*')
    print 'found files: %s' % localpath
    return localpath[0]
# TODO - need to convert milliseconds to seconds to match 
def get_task_stats(queryNum, t):
    """
    Takes a tez .jist file and computes statistics

    Args:
        filename: filename of .jist file

    Returns:
        (stats,read_data,aggregate_data) - a triple where

            stats : is a list of dictionaries of the form 
                    
                    {'duration': duration of task,
                    'end': ending time,
                     'start': starting time,
                     'task_type': 'read_write or aggregate type task'}

                     each element of stats refers to a single task with the above properties

             read_data : number of read/write type of tasks

             aggregate_data : number of aggregate type tasks

    """
    filepath = copy_tez_log_file(queryNum, t)
    # filepath = queryNum
    l =[]
    s =[]
    stats = []

    count =0
    aggregate_data =0
    read_data =0

    with open(filepath) as f:
        a =f.readlines()

    for i in xrange(0, len(a)):
        
        tez_type = json.loads(a[i][:-2])['entitytype']
        # print tez_type
        if tez_type == u'TEZ_TASK_ID' :
            event_type = str(json.loads(a[i][:-2])['events'][0]['eventtype'])
            
            if event_type == u'TASK_FINISHED':
                status = json.loads(a[i][:-2])['otherinfo']['status']
                
                if status == 'SUCCEEDED':
                    # print status
                    # appending finished successful tasks
                    l.append(json.loads(a[i][:-2])) 

    for i in l:
        if str(i['otherinfo']['counters']['counterGroups'][1]['counters'][0]['counterName']) != 'REDUCE_INPUT_GROUPS':
            # Prolly read write data
            read_data = read_data+1
            start = i['otherinfo']['startTime']
            end = i['otherinfo']['endTime']
            stats.append({'task_type':'read_write', 'start': start, 'end': end, 'duration': (end -start)})
        else:
            aggregate_data = aggregate_data +1
            start = i['otherinfo']['startTime']
            end = i['otherinfo']['endTime']
            stats.append({'task_type':'aggregation', 'start': start, 'end': end, 'duration': (end -start)})

    
    # half of the tasks attempts are starts and the other half is finished 
    total_number_of_tasks = len(l)                 
    if aggregate_data == 0:
        ratio_tasks = 'undefined'
    else:
        ratio_tasks = float(read_data) / float(aggregate_data)
    # return (a,stats,read_data,aggregate_data,s,l)    
     
    return (total_number_of_tasks, ratio_tasks, stats)    
