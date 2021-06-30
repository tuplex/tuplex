import glob
import os

input_dir = "/disk/data/weblogs/*.*.*.txt"
output_dir =  "/disk/data/weblogs_clean"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for filename in glob.glob(input_dir):
    new_filename = f'{output_dir}/{filename[filename.rfind("/")+1:]}'
    print(f'{filename} -> {new_filename}')
    with open(filename, "r", encoding='latin_1') as f:
        with open(new_filename, 'w') as fo:
            for l in f:
                test = l.encode('ascii', 'replace').decode('ascii') # make it ascii
                test = test.replace('\0', '?') # get rid of null characters
                fo.write(test)

