import json
import sys

def parse_map_reduce_hist(filename):
    """
    """


    with open(filename) as f:
        a =f.readlines()

    k =0    
    start_count = 0
    end_count = 0    
    for i in a:
        # print i

        if 'MapReduce Jobs Launched:' in i: 
            start_count = k
            # print i

        if 'Total MapReduce CPU Time Spent:' in i:
            end_count = k
            # print i

        k= k+1    

    #   Final output contains the lines as they were
    #   in the original file
    final_output = a[start_count+1:end_count]

    # array of dicts, each element is a dict
    # of the form:
    # {'Maps': '1', 'Stage_name': '5:', 'Reduce': '1'}
    list_of_stages =[] 

    for i in xrange(0, len(final_output)):

        j = final_output[i].split()

        if j[3] == 'Reduce:':
            list_of_stages.append({'Stage_name':j[0].split('-')[2],'Maps':(j[2]),'Reduce':(j[4]), 'data_read': int(j[11]), 'data_written': int(j[14])})
        else:
            list_of_stages.append({'Stage_name':j[0].split('-')[2],'Maps':(j[2]),'Reduce':'none', 'data_read': int(j[9]), 'data_written': int(j[12])}) 


    edge_list =[]        
    for i in xrange(0, len(list_of_stages)-1):
        edge_list.append({'start': list_of_stages[i], 'end': list_of_stages[i+1]})            

    return (edge_list)

def generate_dot_file(edge_list, filename):

    string_list = []
    string_list.append('digraph {\n')


    source = edge_list[0]['start']
    sink = edge_list[0]['end']

    s = make_bubble_for_one_node(source, sink, True)
    string_list.append(s)

    for i in xrange(1, len(edge_list)):


        source = edge_list[i]['start']
        s = make_bubble_for_one_node(sink, source, True)
        sink = edge_list[i]['end']

        s = make_bubble_for_one_node(source, sink, True)
        string_list.append(s)


    source = edge_list[len(edge_list)-1]['start']
    sink = edge_list[len(edge_list)-1]['end']

    s = make_bubble_for_one_node(sink, sink, False)
    string_list.append(s)

    string_list.append('}')

    name = "%s.dot"%(filename)
    text_file = open(name, "w")
    for i in string_list:
        text_file.write(i)
    
    text_file.close()

def make_bubble_for_one_node(source, sink, flag):
    
    if flag == True:
        count = 2
        final_string=""

        if source['Reduce'] != 'none':
            count = 3

        for i in xrange(0,count):
            if count == 3:
                if i == 0:
                    final_string += '    "HDFS stage %s"-> "stage %s num_mappers %s"; \n'%(source['Stage_name'], source['Stage_name'], source['Maps'])             

                if i == 1:
                    final_string += '    "stage %s num_mappers %s" -> "stage %s num_reducers %s"; \n'%(source['Stage_name'], source['Maps'],source['Stage_name'], source['Reduce'])                

                if i == 2:
                    final_string += '    "stage %s num_reducers %s" -> "HDFS stage %s"; \n'%(source['Stage_name'], source['Reduce'],sink['Stage_name'])                                                
            
            # no reduce
            else:
                if i == 0:   
                    final_string += '    "HDFS stage %s" -> "stage %s num_mappers %s"; \n'%(source['Stage_name'],source['Stage_name'], source['Maps']) 

                if i == 1:
                    final_string += '    "stage %s num_mappers %s" -> "HDFS stage %s"; \n'%(source['Stage_name'], source['Maps'],sink['Stage_name'])    

    else:
        count = 2
        final_string=""

        if source['Reduce'] != 'none':
            count = 3

        for i in xrange(0,count):
            if count == 3:
                if i == 0:
                    final_string += '    "HDFS stage %s"-> "stage %s num_mappers %s"; \n'%(source['Stage_name'], source['Stage_name'], source['Maps'])             

                if i == 1:
                    final_string += '    "stage %s num_mappers %s" -> "stage %s num_reducers %s"; \n'%(source['Stage_name'], source['Maps'],source['Stage_name'], source['Reduce'])                

                if i == 2:
                    final_string += '    "stage %s num_reducers %s" -> "HDFS"; \n'%(source['Stage_name'], source['Reduce'])                                                
            
            # no reduce
            else:
                if i == 0:   
                    final_string += '    "HDFS stage %s" -> "stage %s num_mappers %s"; \n'%(source['Stage_name'],source['Stage_name'], source['Maps']) 

                if i == 1:
                    final_string += '    "stage %s num_mappers %s" -> "HDFS"; \n'%(source['Stage_name'], source['Maps'])    


    return final_string

if __name__ == '__main__':
    e= parse_map_reduce_hist('./output/tpcds_query85_mr.out')
    generate_dot_file(e, './dotfiles/query85mr')    
