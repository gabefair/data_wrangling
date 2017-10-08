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

chunksize = 45000 #reddit posts have a 15000 char limit except for self-post subreddit gets 40k, so we will just make it 45k right now to include metadata.
current_comment_count = 0
global_comment_count = 0
file_count = 0
comments_per_file = 20000
json_pattern = re.compile(b'\{(?:[^{}])*\}')
file_progress = 0
chunk_remainder = 0
output_file_contents = ''  # The new file will build in RAM before writing to disk. Limiting the number of disk bottlenecks
file_size = 0
bar = object()

def split_json(file_argument):
	global output_file_contents
	global chunk
	global current_comment_count
	global file_progress
	global chunk_remainder
	last_chunk = False
	global json_pattern
	global bar
	global global_comment_count
	global file_size
	file_size = os.path.getsize(sys.argv[1])
	bar = progressbar.ProgressBar(maxval=100,widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

	print("Reading file: " + file_argument + " Splitting every: " + str(comments_per_file) + " comments")
	with open(file_argument, "rb") as file:
		bar.start()
		bar.update(1)
		chunk = file.read(chunksize)
		
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
					#print("current_comment_count: "+ str(current_comment_count) + " sys.getsizeof(str(match.group())): " + str(sys.getsizeof(str(match.group())))+ " file_progress: "+ str(file_progress)+'\n')
				else:
					print("ERROR: no match found in: \n" + chunk)
					exit()

				if (current_comment_count >= comments_per_file):
					write_file(file_argument, file_count, output_file_contents)
					output_file_contents = ''

			chunk = chunk[chunk_remainder:] + grab_new_chunk(file,chunksize)
			if not chunk or chunk == "":
				last_chunk = True
				break
		write_file(file_argument, file_count, output_file_contents)
		bar.finish()
		print("Bytes successfully read:  "+ str(int(file_progress)) + '/' + str(os.path.getsize(file_argument)) + ' ('+ str((file_progress//os.path.getsize(file_argument))*100) + '%)')
		print("Total files: ", file_count)
		print("Total comments: ", global_comment_count)
		return


def write_file(file_name, file_num, output):
	global current_comment_count
	global file_progress
	global file_count
	global global_comment_count
	
	f = open(file_name + '_%04d' % file_num, 'w')
	f.write(output)
	f.close()
	file_count += 1
	current_comment_count = 0


def grab_new_chunk(file,byte_size_of_chunk):
	global file_progress
	global bar
	global file_size
	
	file_progress = int((file.tell()/file_size)*100)
	bar.update(file_progress)
	#file.seek(byte_size_of_chunk,0)
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