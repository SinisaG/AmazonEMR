# 1. script creates input file
# 2. input file/s, reducer and mapper are uploaded to s3 bucket
# 3. Then emr is created
# 4. Steps for erm is init
# 5. Erm steps are executed
# 6. We fetch the result from output folder
# 7. Print result to console

import time
from boto.emr import StreamingStep
import boto
from boto.s3.key import Key

def upload_to_bucket(filename, folder=""):
    k = Key(bucket)
    k.key = folder + "/" + filename
    k.set_contents_from_filename(filename)

def create_input_file(filename, range_start, range_finish):
    file = open(filename, "w")
    for i in range(range_start, range_finish):
        file.write(str(i)+"\n")
    file.close()

AWS_ACCESS_KEY_ID = 'XXX'
AWS_SECRET_ACCESS_KEY = 'XXX'
bucket_name = 'bucketname'
testfile = "numbers"
testfile2 = "numbers2"
mapper = "mapper.py"
reducer = "reducer.py"
jobname = "My ERM"
input_folder = "input"
output_folder = "output/result"

conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
   AWS_SECRET_ACCESS_KEY)
try:
    bucket = conn.get_bucket(bucket_name)
except Exception, e:
    bucket = conn.create_bucket(bucket_name)

print "Target bucket is %s" % bucket_name

print "Uploading mapper to bucket. Mapper: %s" % mapper
upload_to_bucket("mapper.py")
print "Mapper uploaded"

print "Uploading reducer to bucket. Reducer: %s" % mapper
upload_to_bucket("reducer.py")
print "Reducer uploaded"

print "Creating input file %s" % testfile
create_input_file(testfile, 0, 50)
print "Input file created"
print "Uploading input to bucket. Input: %s" % testfile
upload_to_bucket(testfile, input_folder)

print "Creating input file %s" % testfile2
create_input_file(testfile2, 25, 61)
print "Input file created"
print "Uploading input to bucket. Input: %s" % testfile2
upload_to_bucket(testfile2, input_folder)

print "Init emr connection"
conn = boto.connect_emr(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


print "Setting up streamStep"
result = output_folder + str(time.time())
step = StreamingStep(name='My word example',
                     mapper='s3n://%s/%s' % (bucket_name, mapper),
                     reducer='s3n://%s/%s' % (bucket_name, reducer),
                     input='s3n://%s/%s' % (bucket_name, input_folder),
                     output='s3n://%s/%s' % (bucket_name, result))


print "Starting ERM job %s" % jobname
jobid = conn.run_jobflow(name=jobname, steps=[step], log_uri="s3://"+bucket_name+"/logs/", enable_debugging = True)

status = conn.describe_jobflow(jobid).state

while not (status == 'COMPLETED'):
    status = conn.describe_jobflow(jobid).state
    print "Status: %s, Job Id: %s" % (status, jobid)
    time.sleep(10)

print "Job finished"

print "Result:"
result_file = 'result'
bucket_list = bucket.list()
for l in bucket_list:
    print key_string
    key_string = str(l.key)
    if result in key_string:
        l.get_contents_to_filename(result_file)

file = open(result_file, "r")
for line in file:
    print line,