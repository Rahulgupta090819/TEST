import re,random,os,sys,getopt,subprocess,glob,stat
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str,
                    help="Path of the mounted volume")
parser.add_argument("--type", type=str,
                    help="The iteration number")
parser.add_argument("--incremental_number", type=int, default=0,
                    help="The iteration number")
					
args = parser.parse_args()

if (args.type == 'incremental' and args.incremental_number == 0 ):
    print "Please provide the incremental number\n"

args.path = args.path + "/root_dir"	
 
print "SHAN: Path recieved in generate tracker " +  args.path

def baseline_tracker_generate(path):
    baselinefile = args.path + "/index_file_baseline"
    trackerfile = args.path + "/trackerfile.txt"	
    root_dir_entry = args.path 	
	
    trackerfile_fh = open(trackerfile,'w')

    with open(baselinefile) as baselinefile_fh:
        for line in baselinefile_fh:
            matchobj = re.match('File_Created:(.*)', line)
            filepath = matchobj.group(1)
        
            temp_str = filepath + " :version=1 :modified= :deleted= \n"
            trackerfile_fh.write(temp_str)
	
    ## Adding the tracker file entry 	
    temp_str = trackerfile + " :version=1 :modified= :deleted= \n"
    trackerfile_fh.write(temp_str)

    ## Adding the json.txt file entry 	
    json_file = args.path + "/json.txt"	
    temp_str = json_file + " :version=1 :modified= :deleted= \n"
    trackerfile_fh.write(temp_str)	
	
    ## Adding the tracker file entry 	
    temp_str = baselinefile + " :version=1 :modified= :deleted= \n"
    trackerfile_fh.write(temp_str)
	
    ## Adding the baseline file entry 	
    temp_str = root_dir_entry + " :version=1 :modified= :deleted= \n"
    trackerfile_fh.write(temp_str)
	
    trackerfile_fh.close()
## END of baseline_tracker_generate
	
	
def incremental_tracker_merge(path,incremental_number):
    trackerfile = path + "/trackerfile.txt"
    # trackerfile_fh = open(trackerfile,'w')
	
    increfile = path + '/index_file_incremental_' + str(incremental_number)    
    # increfile_fh  	
	
    incremental_number = incremental_number + 1	
    with open(trackerfile) as trackerfile_fh:
    
        dict_temp = {}
        for line in trackerfile_fh:
            line = line.rstrip()		
  
            (key, version, modified, deleted) = line.split(" :")
            dict_temp[key] = {} 	    
            # dict_temp[key]['version'] = version + ',' + str(incremental_number) 
            dict_temp[key]['modified'] = str(modified)
            dict_temp[key]['deleted'] = str(deleted)
		    
            if(dict_temp[key]['deleted'] == "deleted="):				
               dict_temp[key]['version'] = version + ',' + str(incremental_number) 
            else:
               dict_temp[key]['version'] = version  
                			
	
    trackerfile_fh.close()	
    
    with open(increfile) as increfile_fh:
        for line in increfile_fh:
            matchobj = re.match('(.*?):(.*)', line)
            file_op = matchobj.group(1)
            filepath = matchobj.group(2)
			
            if(file_op == "File_Modified"):
                if filepath in dict_temp:
                    temp_str = dict_temp[filepath]['modified'] + "," + str(incremental_number)
                    dict_temp[filepath]['modified'] = temp_str					

			
            if(file_op == "File_Deleted"):
                if filepath in dict_temp:
                    temp_str = dict_temp[filepath]['deleted'] +  str(incremental_number)
                    dict_temp[filepath]['deleted'] = temp_str	
                    temp_str2 = dict_temp[filepath]['version'] 
                    temp_str2 = temp_str2[:-2]
                    dict_temp[filepath]['version'] = temp_str2						
					
            if(file_op == "File_Created"):
                temp_str =  "version=" + str(incremental_number)
                dict_temp[filepath] = {}
                dict_temp[filepath]['version'] = "version=" + str(incremental_number)
                dict_temp[filepath]['modified'] = "modified="
                dict_temp[filepath]['deleted'] = "deleted="                				

    ## Adding the incremental files into the tracker file				
    dict_temp[increfile] = {}
    dict_temp[increfile]['version'] = "version=" + str(incremental_number)
    dict_temp[increfile]['modified'] = "modified="
    dict_temp[increfile]['deleted'] = "deleted="  
		
    trackerfile_fh_2 = open(trackerfile, "w")	
    for k, v  in dict_temp.items():
        trackerfile_fh_2.write(str(k) + " :" + dict_temp[k]['version'] + ' :' + dict_temp[k]['modified'] + ' :' + dict_temp[k]['deleted']  + '\n')	
	
    trackerfile_fh_2.close()		

    ## For debugging purpose only, storing the trackerfile in /tmp	. Delete if not necessary
    debug_trackerfile_fh = open("/tmp/trackerfile.txt", "w")	
    for k, v  in dict_temp.items():
        debug_trackerfile_fh.write(str(k) + " :" + dict_temp[k]['version'] + ' :' + dict_temp[k]['modified'] + ' :' + dict_temp[k]['deleted']  + '\n')	
	
    debug_trackerfile_fh.close()			


if (args.type == 'baseline'):
    baseline_tracker_generate(args.path)
if (args.type == 'incremental'):
    incremental_tracker_merge(args.path, args.incremental_number)
	



