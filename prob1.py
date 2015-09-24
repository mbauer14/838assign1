#!/usr/bin/python
import subprocess
import procutils
import tezutils
import mrutils
import os
import json
#queryList = ['12', '21', '50', '71', '85']
#types = ['mr', 'tez']

queryList = ['85']
types = ['tez']
vms = ['vm-1', 'vm-2', 'vm-3', 'vm-4']

def callScript(queryNum, t, logfile):
    script = './run_query_hive_%s.sh' % t
    print 'Running script: %s > %s' % (script, logfile)
    f = open(logfile, 'w')
    subprocess.call([script, queryNum], stdout=f)

def output_to_file(queryNum, t, results):
    filepath = './plotdata/%s%s' % (queryNum, t)
    if os.path.exists(filepath):
        os.remove(filepath)

    import json
    with open(filepath, 'w') as outfile:
        json.dump(results, outfile)

    import pprint 
    print 'QueryNum: %s t: %s' % (queryNum, t)
    pp = pprint.PrettyPrinter(indent=3)
    pp.pprint(results)

def emptyBufferCaches():
    for vm in vms:
    # Execute ssh command, get results
        cat_output = subprocess.check_output(['ssh', vm, './clear_cache.sh'])  

def runQuery(queryNum, t):
    """
        Runs a specific query and stores all results to csv file for later graphing.
        Accepts a queryNumber and a type t (hive/mr) which will be used to call the correct run script.
        Takes initial measurements for network and disk
        Runs the query, echoes to a file
        When complete, takes final network/disk measurements.
        Parses the file and returns start/end time in unixtimestamps
        Calls hadoop dfs to get a bunch of task statistics
        Stores all stats in a csv file
    """
    # Deleting old stuff
    mrutils.remove_all_mr_logs() 
    tezutils.remove_all_tez_logs() 
    
    # Get the initial stats for all vms
    print 'Getting initial stats.'
    start_stats = procutils.get_all_stats() 
    print 'Initial stats complete.'
    
    # Will call the script that performs the query, output the result of that query to a file
    logfile = './stdoutput/%s%s' % (queryNum, t)
    # Remove old file if exists
    if os.path.exists(logfile):
        os.remove(logfile)
    print 'logfile: %s' % logfile
    callScript(queryNum, t, logfile)
    print 'script complete'
    
    # Get the final stats
    print 'Getting final stats.'
    stop_stats = procutils.get_all_stats()
    print 'Final stats complete.'

    # Calculate the differences between 
    print 'Calculating diff_stats'
    diff_stats = procutils.calc_stats_diff(start_stats, stop_stats)
    print 'Finishing diff_stats'

    time_elapsed = procutils.read_time_stamps(logfile)

    print 'get_task_stats start'
    if (t == 'mr'):
        total_num_tasks, ratio_tasks, task_distribution = mrutils.get_task_stats(queryNum, t)
    else:
        total_num_tasks, ratio_tasks, task_distribution = tezutils.get_task_stats(queryNum, t)
    print 'get_task_stats finish'
    
   
     
    results = {'disknet': diff_stats,
                'time_elapsed': time_elapsed,
                'total_num_taks': total_num_tasks,
                'ratio_tasks': ratio_tasks,
                'task_distribution': task_distribution
            }

    # Echo everything to a csv file
    output_to_file(queryNum, t, results)

if __name__ == "__main__":
    # Perform some cleanup stuff
    # remove the results/logs directories

    # remove the logs directory

    # Create the logs/results directory
    # Remote the entire /tmp/hadoop-yarn/staging/history


    print 'Starting problem 1 generation.'
    for query in queryList:
        for t in types:
            # empty the buffer caches on each vm
            emptyBufferCaches()
            print t + query
            runQuery(query, t)
