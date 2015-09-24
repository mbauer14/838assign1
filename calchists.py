import json
import sys
import os
#queryList = ['12', '21', '50', '71', '85']
#types = ['mr', 'tez']

queryList = ['12', '21', '50', '71', '85']
types = ['mr', 'tez']

def convertToSeconds(string, number):
    #if 'mr' in string:
    #    return number
    #else: 
    return int(number / 1000)

def output_to_file(string, data):
    filepath = './dists/%s' % string
    if os.path.exists(filepath):
        os.remove(filepath)

    with open(filepath, 'w') as outfile:
        json.dump(data, outfile)

def calcDistribution(string):
    filepath = './plotdata/%s' % string
    with open(filepath) as data_file:    
        results = json.load(data_file)
    
    if 'mr' in string:
        startIndex = 'startTime'
        stopIndex = 'finishTime'
        tasks = results['task_distribution'].values()
    else:
        startIndex = 'start'
        stopIndex = 'end'
        tasks = results['task_distribution']
    
    
    minTime =  sys.maxint
    maxTime = 0

    # Get the total duration
    for task in tasks:
        task[startIndex] = convertToSeconds(string, task[startIndex]) 
        task[stopIndex] = convertToSeconds(string, task[stopIndex]) 
        if task[startIndex] < minTime:
            minTime = task[startIndex]
        if task[stopIndex] > maxTime:
            maxTime = task[stopIndex]

    distribution = []
    currTime = minTime

    for i in range(0, maxTime - minTime + 1):
        timecount = 0
        for task in tasks:
            if task[startIndex] <= currTime and task[stopIndex] > currTime:
                timecount += 1
   
        distribution.append(timecount) 
        currTime += 1


    output_to_file(string, distribution)
    #print (len(distribution)) 
    #print distribution

if __name__ == "__main__":

    for query in queryList:
        for t in types:
            string = '%s%s' % (query, t)
            print t + query
            calcDistribution(string)

