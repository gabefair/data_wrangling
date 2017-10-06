# Gabriel Fair
# Please suggest any improvements: http://keybase.io/gabefair
import mmap
import re
import argparse
import sys
import progressbar
from time import sleep
import os

# parser = argparse.ArgumentParser(description='Splits giant file with many JSON objects.')
# parser.add_argument('json file', metavar='F', type=open, help='a file containing valid json' required=True)

chunksize = 45000 #reddit posts have a 15000 char limit except for self-post subreddit gets 40k, so we will just make it 45k right now to include metadata.
comment_count = 0
file_count = 0
comments_per_file = 20000
json_pattern = re.compile(b'\{(?:[^{}])*\}')
file_progress = 0
read_window = (0, chunksize)  # starting with window the size of the byte sector on the disk
output_file_contents = ''  # The new file will build in RAM before writing to disk. Limiting the number of disk bottlenecks


def split_json(file_argument):
	global output_file_contents
	global chunk
	global comment_count
	global file_progress
	global read_window
	last_chunk = False
	global json_pattern

	print("Reading file: " + file_argument + " Splitting every: " + str(comments_per_file) + " comments")
	with open(file_argument, "rb") as file:
		bar = progressbar.ProgressBar(maxval=os.path.getsize(file_argument),widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
		bar.start()
		# file_map = mmap.mmap(file.fileno(), chunksize)
		chunk = file.read(chunksize)
		
		while not last_chunk:
			for match in json_pattern.finditer(chunk):
				if match:
					# print("Found match!: " + str(match.group())[1:])
					if (comment_count >= comments_per_file-1):
						output_file_contents = output_file_contents + str(match.group())[1:]# slice is to remove a stray 'b' that appears at the start of the match
					else:
						output_file_contents = output_file_contents + str(match.group())[1:] + '\n'	 # slice is to remove a stray 'b' that appears at the start of the match
					chunk = file.seek(chunksize, 1)
					comment_count += 1
					file_progress += match.end()
				else:
					print("ERROR: no match found in: \n" + chunk)
					exit()

				if (comment_count >= comments_per_file):
					bar.update(file_progress)
					write_file(file_argument, file_count, output_file_contents)
					output_file_contents = ""

			chunk = grab_new_chunk(file,chunksize)
			if not chunk or chunk == "":
				print("arrived at last_chunk")
				last_chunk = True
				break
		write_file(file_argument, file_count, output_file_contents)
		bar.finish()


def write_file(file_name, file_num, output):
	global comment_count
	global file_progress
	global read_window
	global file_count
	f = open(file_name + '_%05d' % file_num, 'w')
	f.write(output)
	f.close()
	file_count += 1
	comment_count = 0


def grab_new_chunk(file,byte_size_of_chunk):
	return file.read(chunksize)


def main():
	# args = parser.parse_args()
	split_json(sys.argv[1])


if __name__ == '__main__':
	main()