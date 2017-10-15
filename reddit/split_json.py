# Gabriel Fair
# Please suggest any improvements: http://keybase.io/gabefair
import mmap
import re
import argparse
import sys
import progressbar
from time import sleep
import os
from time import clock
import atexit
from time import time
from datetime import timedelta
from time import perf_counter

chunksize = 50000 #.05mb loaded into RAM at a time #Ironically the smaller the chunksize the faster it goes b/c of the regular expression matching being O(n^2)
current_comment_count = 0
global_comment_count = 0
file_count = 0
comments_per_file = 500000
json_pattern = re.compile(b'\{(?:[^{}])*\}')
file_progress = 0
chunk_remainder = 0
output_file_contents = ''  # The new file will build in RAM before writing to disk. Limiting the number of disk bottlenecks
file_size = 0
bar = progressbar.ProgressBar(maxval=100,widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

def split_json(file_argument):
	global output_file_contents
	global current_comment_count
	global file_progress
	global chunk_remainder
	last_chunk = False
	global json_pattern
	global bar
	global global_comment_count
	global file_size
	
	file_size = os.path.getsize(sys.argv[1])

	print("Reading file: " + file_argument + " Splitting every: " + str(comments_per_file) + " comments")
	bar = progressbar.ProgressBar(redirect_stdout=True)
	bar.start()
	lap_time = perf_counter()
	with open(file_argument, "rb") as file:
		chunk = grab_new_chunk(file)
		
		while not last_chunk:
			for match in json_pattern.finditer(chunk):
				#print("A match has been found: \n" + str(match.group())[2:-1])
				if match:
					current_comment_count = current_comment_count + 1
					global_comment_count = global_comment_count + 1
					output_file_contents = output_file_contents + str(match.group())[2:-1]# 
					if (current_comment_count < comments_per_file):
						output_file_contents = output_file_contents + '\n'
					chunk_remainder = match.end()
					if ( current_comment_count % 500000 == 0):
						print("Comments proccessed: "+ str(global_comment_count) + ' and the time it took: ' + str(timedelta(seconds=perf_counter() - lap_time)))
						lap_time = perf_counter()
						bar.update(int((file.tell()/file_size)*100))
				else:
					print("ERROR: no match found in: \n" + chunk)
					exit()

				if (current_comment_count >= comments_per_file):
					write_file(file_argument, 0)
			#print("\n\t Old chunk: \n")
			#print(chunk)
			new_chunk = grab_new_chunk(file)
			if not new_chunk or new_chunk == "":
				last_chunk = True
				break
			chunk = chunk[chunk_remainder:] + new_chunk #append new chunk to any leftovers
			#print("\n\t New chunk: \n")
			#print(chunk)
			
		write_file(file_argument, 0)
		bar.finish()
		print("Bytes successfully read:  "+ str(int(file.tell())) + '/' + str(os.path.getsize(file_argument)) + ' ('+ str((file.tell()//os.path.getsize(file_argument))*100) + '%)')
		print("Total files: ", file_count)
		print("Total comments: ", global_comment_count)
		return


def write_file(file_name, leave_open_flag):
	global current_comment_count
	global file_count
	global global_comment_count
	global output_file_contents
	
	f = open(file_name + '_%04d' % file_count, 'a')
	f.write(output_file_contents)
	f.close()
	output_file_contents = ''
	if(leave_open_flag == 0):
		file_count += 1
		current_comment_count = 0


def grab_new_chunk(file):
	global file_progress
	global bar
	global file_size
	write_file(sys.argv[1],1)#save_current_progress
	#print("file.tell is: ", file.tell())
	file_progress = int((file.tell()/file_size)*100)
	bar.update(file_progress)
	#print("updated bar")
	return file.read(chunksize)
	
	
def secondsToStr(t):
    return str(timedelta(seconds=t))

line = "="*40
def log(s, elapsed=None):
    print(line)
    print(secondsToStr(time()), '-', s)
    if elapsed:
        print("Elapsed time:", elapsed)
    print(line)
    print()

def endlog(start):
    end = time()
    elapsed = end-start
    log("End Program", secondsToStr(elapsed))

def now():
    return secondsToStr(time())


def main():
    # parser = argparse.ArgumentParser(description='Splits giant file with many JSON objects.')
	# parser.add_argument('json file', metavar='F', type=open, help='a file containing valid json' required=True)
	# args = parser.parse_args()
	start_time = time()
	atexit.register(endlog)
	log("Start Program")
	split_json(sys.argv[1])
	endlog(start_time)


if __name__ == '__main__':
	main()