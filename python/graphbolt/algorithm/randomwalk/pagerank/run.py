#!/usr/bin/env python3
__copyright__ = """ Copyright 2018 Miguel E. Coimbra

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. """
__license__ = "Apache 2.0"

# Used to launch combinations of parameters for different GraphBolt 
# PageRank executions.

###########################################################################
################################# IMPORTS #################################
###########################################################################

# PEP 8 Style Guide for imports: https://www.python.org/dev/peps/pep-0008/#imports
# 1. standard library imports
import argparse
from collections import OrderedDict
import datetime
import getpass
import glob
from io import TextIOWrapper
import os
import pathlib # https://docs.python.org/3.6/library/pathlib.html#pathlib.Path.mkdir
import shlex # https://docs.python.org/3/library/shlex.html#shlex.quote
import shutil
import signal
import socket
import string
import subprocess
import sys
import tempfile
import threading
import time
from typing import Dict, List

# 2. related third party imports
import psutil
import pytz
from google.api_core.protobuf_helpers import get_messages
from google.cloud import storage

# 3. custom local imports
#from graphbolt.evaluation.rbo import batch_rank_evaluator
from graphbolt.evaluation.rbo import batch_rank_evaluator
from graphbolt.stream import streamer
from graphbolt import localutil

###########################################################################
######################### GOOGLE CLOUD FUNCTIONS ##########################
###########################################################################

def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""

    # Explicitly use service account credentials by specifying the private key
    # file.
    #storage_client = storage.Client.from_service_account_json('creds.json')
    storage_client = storage.Client()

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    #returns a public url
    return blob.public_url

def upload_local_directory_to_gcs(local_path, bucket, gcs_path, skip_upload: bool = False):
    #assert os.path.isdir(local_path)

    if os.path.isdir(local_path):
        for local_file in glob.glob(local_path + '/**'):
            if not os.path.isfile(local_file):
                upload_local_directory_to_gcs(local_file, bucket, gcs_path + "/" + os.path.basename(local_file))
 #       else:
    else:
        #remote_path = os.path.join(gcs_path, local_file[1 + len(local_path):])

        #print("remote_path:\t{}".format(remote_path))
        #print("local_path:\t{}".format(local_path))
        #exit(0)

        print("gcs_path:{}".format(gcs_path))
        print("local_path:{}".format(local_path))

        blob = bucket.blob(gcs_path)
        #blob.upload_from_filename(local_file)
        blob.upload_from_filename(local_path)

        print('File {} uploaded to {}.'.format(
            local_path,
            gcs_path))

def upload_zip_files(target_path: str, bucket_name: str, archive_keyword: str, skip_upload: bool = False):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    archive_root = target_path[:target_path.find("testing")+len("testing")]
    results_zip_base_name = target_path[target_path.rfind(os.path.sep) + 1:]
    results_zip_path = os.path.join(ZIP_DIR, results_zip_base_name + "_" + archive_keyword)
    archive_base = target_path[target_path.find(archive_keyword):]
    shutil.make_archive(results_zip_path, 'zip', archive_root, archive_base)



    remote_file_name = results_zip_path[results_zip_path.rfind(os.path.sep)+1:] + ".zip"

    print("local_zip: " + results_zip_path + ".zip")
    print("bucket: " + bucket_name)
    print("archive_base: " + "testing/" + remote_file_name)
    
    if not skip_upload:
        upload_local_directory_to_gcs(results_zip_path + ".zip", bucket, "testing/" + remote_file_name)

###########################################################################
##################### CHILD PROCESS CLEANUP ROUTINES ######################
###########################################################################

streamer_server = None



def kill_proc_tree(pid: int, including_parent: bool = True) -> None:
    # See https://stackoverflow.com/questions/1230669/subprocess-deleting-child-processes-in-windows/4229404?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
    # http://psutil.readthedocs.io/en/latest/
    print("> Checking tree of child PID {}.".format(pid))
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    for child in children:
        print("> Killing grandchild process {}.".format(child.pid))
        child.kill()
    #gone, still_alive = psutil.wait_procs(children, timeout=5)
    #_, _ = psutil.wait_procs(children, timeout=5)
    psutil.wait_procs(children, timeout=5)
    if including_parent:
        print("> Killing parent process {}.".format(pid))
        parent.kill()
        parent.wait(5)


def register_grandchildren(pid: int, processes: List) -> None:
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    for child in children:
        print("> Adding grandchild {}.".format(child.pid))
        processes.append(child.pid)


def cleanup(processes: List) -> None:
    for pid in process_list:
        if psutil.pid_exists(pid): #https://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid-in-python
            #term(pid)
            kill_proc_tree(pid)
        else:
            print("> Process {} was already closed.".format(pid))

    if not streamer_server is None:
        streamer_server.shutdown()
        streamer_server.out.flush()
        streamer_server.out.close()


# Prepare the handler for SIGINT (Ctrl+C).
def signal_handler(signal, frame) -> None:
    print("\n\n> SIGINT received. Stopping processes' execution...\n")
    cleanup(process_list)
    print("> Done. Exiting.")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


def start_process(command: str, stdout: TextIOWrapper = None, stderr: TextIOWrapper = None) -> subprocess.Popen:
    
    # Subprocess standard output will be the calling script's standard output if none is provided.
    if stdout is None:
        stdout = sys.stdout

    # Subprocess standard error will be set to its standard output if none is provided.
    if stderr is None:
        stderr = stdout

    # Need to use startupinfo if running on Windows.
    if os.name == "nt":
        si = subprocess.STARTUPINFO()
        si.dwFlags = subprocess.CREATE_NEW_PROCESS_GROUP
        process = subprocess.Popen(shlex.split(command), stdout=stdout, stderr=stderr, startupinfo = si)
    else:
        process = subprocess.Popen(shlex.split(command), stdout=stdout, stderr=stderr)
    return process

###########################################################################
############################# READ ARGUMENTS ##############################
###########################################################################


# The class argparse.RawTextHelpFormatter is used to keep new lines in help text.
DESCRIPTION_TEXT = "GraphBolt approximate processing of graphs over streams"
parser = argparse.ArgumentParser(description=DESCRIPTION_TEXT, formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-rbo-only", help="skip GraphBolt execution but try to generate RBO results.", required=False, action="store_true") # if ommited, default value is false
parser.add_argument("-keep-logs", help="keep Apache Flink logs?", required=False, action="store_true") # if ommited, default value is false
parser.add_argument("-keep-cache", help="keep GraphBolt cache directory?", required=False, action="store_true") # if ommited, default value is false

# GraphBolt/PageRank-specific parameters.
parser.add_argument("-i", "--input-file", help="dataset name.", required=True, type=str)
parser.add_argument("-cache-dir", help="cache directory name.", required=False, type=str, default="")
parser.add_argument("-temp-dir", help="temporary directory name.", required=False, type=str, default="")
parser.add_argument("-flink-address", help="Flink cluster's JobManager address.", required=False, type=str, default="")
parser.add_argument("-flink-port", help="Flink cluster's JobManager port.", required=False, type=int, default=8081)
parser.add_argument("-data-dir", help="dataset directory name.", required=True, type=str, default="")
parser.add_argument("-out-dir", help="base output directory where directories for statistics, RBO results, logging, evaluation and figures will be created.", required=True, type=str, default="")
parser.add_argument("-chunks", "--chunk-count", help="set desired number of chunks to be sent by the streamer.", required=True, type=int, default=50)
parser.add_argument("-size", help="set desired GraphBolt RBO rank length.", required=False, type=int, default=-1000)
parser.add_argument("-p", "--parallelism", help="set desired GraphBolt TaskManager parallelism.", required=False, type=int, default=1)
parser.add_argument("-concurrent-jobs", help="set desired GraphBolt instances to run at the same time. The more, the faster a batch of tests will run.", required=False, type=int, default=1)
parser.add_argument("-damp", "--dampening", help="set desired PageRank dampening factor.", required=False, type=float, default=0.85)
parser.add_argument("-iterations", help="set desired PageRank power-method iteration count.", required=False, type=int, default=30)
parser.add_argument("-max-mem", help="ammount of memory made available to Maven.", required=False, type=int, default=2048)
parser.add_argument("-dump-summary", help="should intermediate summary graphs be saved to disk?", required=False, action="store_true") # if ommited, default value is false
parser.add_argument("-delete-edges", help="should edge deletions be sent in the stream?", required=False, action="store_true") # if ommited, default value is false
parser.add_argument("-periodic-full-dump", help="should GraphBolt save all vertex results periodically?", required=False, action="store_true") # if ommited, default value is false

parser.add_argument("-summarized-only", help="running only the approximate version.", required=False, action="store_true")

parser.add_argument("-skip-upload", help="should we skip .zip upload to gcloud?", required=False, action="store_true")

parser.add_argument('-l','--list', nargs='+', help='<Required> Set flag', required=False, default=None)



args = parser.parse_args()

# Sanitize arguments and exit on invalid values.

if (args.list != None) and (len(args.list) <= 0 or (len(args.list) % 3 ) != 0):
    print("> Big vertex parameter list must be a multiple of three. Exiting.")
    sys.exit(1)
if args.flink_port <= 0 or not isinstance(args.flink_port, int):
    print("> '-flink_port' must be a positive integer. Exiting.")
    sys.exit(1)
if args.chunk_count <= 0 or not isinstance(args.chunk_count, int):
    print("> '-chunks' must be a positive integer. Exiting.")
    sys.exit(1)
if args.data_dir.startswith('~'):
    args.data_dir = os.path.expanduser(args.data_dir).replace('\\', '/')
if not (os.path.exists(args.data_dir) and os.path.isdir(args.data_dir)):
    print("> '-data_dir' must be an existing directory.\nProvided: {}\nExiting.".format(args.data_dir))
    sys.exit(1)
if args.iterations <= 0 or not isinstance(args.iterations, int):
    print("> '-iterations' must be a positive integer. Exiting.")
    sys.exit(1)
if args.dampening <= 0 or not isinstance(args.dampening, float):
    print("> '-dampening' must be a positive float in ]0; 1[. Exiting.")
    sys.exit(1)
if args.parallelism <= 0 or not isinstance(args.parallelism, int):
    print("> '-parallelism' must be a positive integer. Exiting.")
    sys.exit(1)
if args.concurrent_jobs <= 0 or not isinstance(args.concurrent_jobs, int):
    print("> '-concurrent_jobs' must be a positive integer. Exiting.")
    sys.exit(1)
#if (args.size <= 0 and args.size_percent < 0) or not isinstance(args.size, int):
if args.size <= 0 or not isinstance(args.size, int):
    print("> '-size' must be a positive integer. Exiting.")
    sys.exit(1)
if args.max_mem <= 0 or not isinstance(args.max_mem, int):
    print("> '-max-mem' must be a positive integer. Exiting.")
    sys.exit(1)
if args.out_dir.startswith('~'):
    args.out_dir = os.path.expanduser(args.out_dir).replace('\\', '/')
if len(args.out_dir) == 0:
    print("> '-out-dir' must be a non-empty string. Exiting")
    sys.exit(1)

if len(args.data_dir) > 0 and not (os.path.exists(args.data_dir) and os.path.isdir(args.data_dir)):
    print("> '-data-dir' must be an existing directory.\nProvided: {}\nExiting.".format(args.data_dir))
    exit(1)

print("> Arguments: {}".format(args))

# Each summarized execution has two process steps: 1 - execute GraphBolt; 2 - execute python RBO evaluation.
# If the sequence "Step 1 -> Step 2" for summarized GraphBolt execution 'i' is self-contained in a single process 'Si', then we are free to start as many 'Si' concurrently as the hardware allows...

###########################################################################
########################### CONSTANTS AND SETUP ###########################
###########################################################################

# Child process list, used to kill everything when this script receives Ctrl+C
process_list = []

# Define some execution constants depending on the machine we're running.
current_user = getpass.getuser()

print("> Running as user '{}' on operating system '{}'\n".format(current_user, os.name))

# Maybe remove the PYTHON_PATH variable.
if os.name == "posix":
    PYTHON_PATH = "/home/{}/bin/python3/bin/python".format(current_user)
    SHELL_DIR = args.out_dir + "/bash/pagerank"
    SHELL_FILETYPE=".sh"
    SHELL_COMMENT="#"
elif os.name == "nt":
    PYTHON_PATH = "C:/Users/{}/AppData/Local/Programs/Python/Python36/python.exe".format(current_user)
    SHELL_DIR = args.out_dir + "/cmd/pagerank"
    SHELL_FILETYPE = ".bat"
    SHELL_COMMENT = "::"
else:
    print("> Sorry {}, your operating system '{}' is not supported. Exiting.".format(current_user, os.name))
    exit(1)

DELETE_TOKEN = 'A'
DELETE_FLAG = ''
if args.delete_edges:
    DELETE_TOKEN = 'D'
    DELETE_FLAG = '-with_deletions'

###########################################################################
########################### CREATE DIRECTORIES ############################
###########################################################################

# Did the user specify a cache directory?
if len(args.cache_dir) == 0:
    CACHE_BASE = os.path.join(args.out_dir, "/cache")
else:
    CACHE_BASE = args.cache_dir

if CACHE_BASE.startswith('~'):
    CACHE_BASE = os.path.expanduser(CACHE_BASE).replace('\\', '/')

if len(args.temp_dir) > 0:
    if args.temp_dir.startswith('~'):
        args.temp_dir = os.path.expanduser(args.temp_dir).replace('\\', '/')
    if not (os.path.exists(args.temp_dir) and os.path.isdir(args.temp_dir)):
        print("> Provided temporary directory does not exist: {}. Exiting".format(args.temp_dir))
        exit(1)
    TEMP_DIR = args.temp_dir


# Make necessary GraphBolt directories if they don't exist.
print("> Checking GraphBolt directories...\n")

if not os.path.exists(EVAL_DIR):
    print("Creating evaluation directory:\t\t'{}'".format(EVAL_DIR))
    pathlib.Path(EVAL_DIR).mkdir(parents=True, exist_ok=True)
else:
    print("Evaluation directory found:\t\t'{}'".format(EVAL_DIR))

if not os.path.exists(STATISTICS_DIR):
    print("Creating statistics directory:\t\t'{}'".format(STATISTICS_DIR))
    pathlib.Path(STATISTICS_DIR).mkdir(parents=True, exist_ok=True)
else:
    print("Statistics directory found:\t\t'{}'".format(STATISTICS_DIR))

if not os.path.exists(RESULTS_DIR):
    print("Creating result directory:\t\t'{}'".format(RESULTS_DIR))
    pathlib.Path(RESULTS_DIR).mkdir(parents=True, exist_ok=True)
else:
    print("Results directory found:\t\t'{}'".format(RESULTS_DIR))

if not os.path.exists(OUT_DIR):
    print("Creating output directory:\t\t'{}'".format(OUT_DIR))
    pathlib.Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
else:
    print("Output directory found:\t\t\t'{}'".format(OUT_DIR))

if not os.path.exists(STREAMER_LOG_DIR):
    print("Creating streamer log directory:\t'{}'\n".format(STREAMER_LOG_DIR))
    pathlib.Path(STREAMER_LOG_DIR).mkdir(parents=True, exist_ok=True)
else:
    print("Streamer log directory found:\t\t'{}'\n".format(STREAMER_LOG_DIR))

if not os.path.exists(SHELL_DIR):
    print("Creating shell command directory:\t'{}'".format(SHELL_DIR))
    pathlib.Path(SHELL_DIR).mkdir(parents=True, exist_ok=True)
else:
    print("Shell command directory found:\t\t'{}'".format(SHELL_DIR))

if not os.path.exists(TEMP_DIR):
    print("Creating temporary directory:\t\t'{}'".format(TEMP_DIR))
    pathlib.Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
else:
    print("Temporary directory found:\t\t'{}'".format(TEMP_DIR))

###########################################################################
########################### CONFIGURE STREAMER ############################
###########################################################################

# Find out a free listening socket to choose a port for the stream server.
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((socket.gethostname(), 0))
STREAM_PORT = s.getsockname()[1]
s.close()

# Build the path to the file to use as a stream of updates.
stream_file_path = "{KW_DATA_DIR}/{KW_DATASET_DIR_NAME}/{KW_DATASET_DIR_NAME}-stream.tsv".format(KW_DATA_DIR = args.data_dir, KW_DATASET_DIR_NAME = args.input_file)

deletion_file_path = ''
optional_deletion_file_path = ''
if args.delete_edges:
    deletion_file_path = "{KW_DATA_DIR}/{KW_DATASET_DIR_NAME}/{KW_DATASET_DIR_NAME}-deletions.tsv".format(KW_DATA_DIR = args.data_dir, KW_DATASET_DIR_NAME = args.input_file)
    optional_deletion_file_path = "-d {KW_DELETION_OPTIONAL_ARG}".format(KW_DELETION_OPTIONAL_ARG = deletion_file_path)

streamer_run_command = '{KW_PYTHON_PATH} -m "graphbolt.stream.streamer" -i "{KW_FILE_STREAM_PATH}" "{KW_DELETIONS_OPTIONAL_PATH}" -q {KW_CHUNK_COUNT} -p {KW_STREAM_PORT}'.format(KW_PYTHON_PATH = PYTHON_PATH, KW_GRAPHBOLT_DIR = GRAPHBOLT_DIR, KW_FILE_STREAM_PATH = stream_file_path, KW_DELETIONS_OPTIONAL_PATH = optional_deletion_file_path, KW_CHUNK_COUNT = args.chunk_count, KW_STREAM_PORT = STREAM_PORT)

if not args.rbo_only:
    # Format current moment as a UTC timestamp.
    now = datetime.datetime.utcnow()
    utc_dt = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute, now.second, tzinfo=pytz.utc)
    timestamp = "Y{}-M{}-D{}-H{}-M{}-S{}-MCRS{}_UTC.log".format(utc_dt.year, utc_dt.month, utc_dt.day, utc_dt.hour, utc_dt.minute, utc_dt.second, utc_dt.microsecond)
    streamer_output_file = "{}/{}_{}".format(STREAMER_LOG_DIR, args.input_file, timestamp)
    print("{}\n".format(streamer_output_file))

    print("> Starting streamer...")

    # Configure the stream properties.
    chunk_size, chunk_sizes, edge_lines, edge_count, deletion_lines = localutil.prepare_stream(stream_file_path, args.chunk_count, deletion_file_path)
    out_file = open(streamer_output_file, "w")

    # Create a server object.
    streamer_server = streamer.make_server(edge_lines=edge_lines, chunk_sizes=chunk_sizes, query_count=args.chunk_count, deletion_lines=deletion_lines, listening_host="localhost", listening_port=STREAM_PORT, out=out_file)

    # Get the server ready to process requests.
    t = threading.Thread(target=streamer_server.serve_forever)
    t.start()
       
    print("> Streamer started")

# Save streamer launch command to shell file. Useful for debugging purposes.
with open(SHELL_DIR + "/" + args.input_file + SHELL_FILETYPE, "w") as shell_file:
    # Format current moment as a UTC timestamp.
    now = datetime.datetime.utcnow()
    utc_dt = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute, now.second, tzinfo=pytz.utc)
    timestamp = str(utc_dt) + " (UTC)"
    
    # Add a sequence of the shell-specific comment character for readibility.
    shell_file.write("{} {}  streamer start\n\n".format(SHELL_COMMENT * 5, timestamp))
    
    # Write the command itself for debugging purposes if we wanted to launch it on its own later.
    shell_file.write('{}\n\n'.format(streamer_run_command))

###########################################################################
######################### CHECK COMPLETE RESULTS ##########################
###########################################################################

# Check if the complete computation results are present, run the complete computation version in case they aren't.
complete_pagerank_result_path = "{KW_RESULTS_DIR}/{KW_DATASET_DIR_NAME}-start_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_complete_{KW_DELETE_TOKEN}".format(
    KW_RESULTS_DIR = RESULTS_DIR, KW_PARALLELISM=args.parallelism, KW_DATASET_DIR_NAME = args.input_file, KW_NUM_ITERATIONS = args.iterations, 
    KW_RBO_RANK_LENGTH = args.size, KW_DAMPENING_FACTOR = args.dampening, KW_DELETE_TOKEN = DELETE_TOKEN)

complete_pagerank_stats_path = complete_pagerank_result_path.replace("Results", "Statistics")

need_to_run_complete_pagerank = False
# Was a results directory found?
if not os.path.exists(complete_pagerank_result_path):
    print("> Complete PageRank results not found:\t{}".format(complete_pagerank_result_path))
    print("> Need to run complete PageRank")
    need_to_run_complete_pagerank = True
else:
    names = os.listdir(complete_pagerank_result_path)
    paths = [os.path.join(complete_pagerank_result_path, name) for name in names]
    sizes = [(path, os.stat(path).st_size) for path in paths]

    # Does the result directory have the expected number of files?
    if len(names) != int(args.chunk_count) + 1:
        print("> Expected {} but got {} complete PageRank results at {}.".format(int(args.chunk_count) + 1, len(names), complete_pagerank_result_path))
        print("> Need to run complete PageRank")
        need_to_run_complete_pagerank = True
    else:
        found_zeros = False
        # Do they all have data?
        for [path, sz] in sizes:
            if sz == 0:
                if os.path.isdir(path):
                    subnames = os.listdir(path)
                    subpaths = [os.path.join(path, name).replace('\\\\', '/') for name in subnames]
                    subsizes = [(s_path, os.stat(s_path).st_size) for s_path in subpaths]

                    if not len(subsizes) == args.parallelism: 
                        found_zeros = True
                        break

                else:
                    found_zeros = True
                    break
        if found_zeros:
            print("> Found complete PageRank results with value of zero at {}.".format(path))
            print("> Need to run complete PageRank")
            need_to_run_complete_pagerank = True
            sys.exit(0)


if args.keep_cache:
    KEEP_CACHE_TEXT = '-keep_cache'
else:
    KEEP_CACHE_TEXT = ""

if args.keep_logs:
    KEEP_LOGS_TEXT = '-keep_logs'
else:
    KEEP_LOGS_TEXT = ""

JOB_MANAGER_WEB_PARAM = "-web"


FLINK_JOB_STATS_FLAG = "-save_flink_operator_stats -save_flink_operator_json"

if len(args.flink_address) > 0:
    FLINK_REMOTE_ADDRESS = "-address " + args.flink_address
    FLINK_REMOTE_PORT = "-port " + str(args.flink_port)
    
else:
    FLINK_REMOTE_ADDRESS = ""
    FLINK_REMOTE_PORT = ""
    #FLINK_JOB_STATS_FLAG = ""

SIZE_STR = '-size ' + str(args.size)
PERIODIC_DUMP_STR = ''
if args.periodic_full_dump:
    PERIODIC_DUMP_STR = '-periodic_full_accuracy'


#TODO: when checking if it's necessary to run PageRank for a given parameter combination (or for complete version), need to check if we are running with periodic full outputs and if the existing Results files are without periodic (or vice-versa). In this case, need to execute.
#TODO: "mvn -f ../pom.xml " requires a graphbolt.localutil function which navigates to the directory of the current file until the python directory is reached. Then, set CWD to that directory


# Build complete PageRank command.
### NOTE: the active code below generates a mvn call which launches a separate process for Java (with its own JVM).
### Article - on running exec:exec - https://www.mojohaus.org/exec-maven-plugin/examples/example-exec-for-java-programs.html
graphbolt_run_command = '''mvn -f ../pom.xml exec:exec -Dexec.executable=java -Dexec.args="-Xmx{KW_MAVEN_HEAP_MEMORY}m -classpath %classpath pt.ulisboa.tecnico.graph.algorithm.pagerank.PageRankMain {KW_FLINK_REMOTE_ADDRESS} {KW_FLINK_REMOTE_PORT} {KW_JOB_MANAGER_WEB_PARAM} {KW_FLINK_JOB_STATS_FLAG} -o '{KW_OUT_BASE}' -cache '{KW_CACHE_BASE}' -damp {KW_DAMPENING_FACTOR:.2f} -iterations {KW_NUM_ITERATIONS} {KW_RBO_RANK_LENGTH} -i '{KW_DATA_DIR}/{KW_DATASET_DIR_NAME}/{KW_DATASET_DIR_NAME}-start.tsv' {KW_KEEP_CACHE} {KW_KEEP_LOGS} -sp {KW_STREAM_PORT} -parallelism {KW_PARALLELISM} -temp '{KW_TEMP_DIR}' {KW_DELETE_FLAG} {KW_PERIODIC_DUMP_STR}"'''.format(KW_MAVEN_HEAP_MEMORY = args.max_mem, KW_FLINK_REMOTE_ADDRESS = FLINK_REMOTE_ADDRESS, KW_FLINK_REMOTE_PORT = FLINK_REMOTE_PORT, KW_JOB_MANAGER_WEB_PARAM = JOB_MANAGER_WEB_PARAM, KW_FLINK_JOB_STATS_FLAG = FLINK_JOB_STATS_FLAG, KW_OUT_BASE = args.out_dir, KW_CACHE_BASE = CACHE_BASE, KW_DAMPENING_FACTOR = args.dampening, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = SIZE_STR, KW_DATA_DIR = args.data_dir, KW_DATASET_DIR_NAME = args.input_file, KW_KEEP_CACHE = KEEP_CACHE_TEXT, KW_KEEP_LOGS = KEEP_LOGS_TEXT, KW_STREAM_PORT = STREAM_PORT, KW_PARALLELISM = args.parallelism, KW_TEMP_DIR = TEMP_DIR, KW_PERIODIC_DUMP_STR = PERIODIC_DUMP_STR, KW_DELETE_FLAG = DELETE_FLAG).replace('\\', '/')

print("{}\n".format(graphbolt_run_command))



# Build path to output directory file.
graphbolt_output_file = '{KW_OUT_DIR}/{KW_DATASET_DIR_NAME}_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_{KW_DELETE_TOKEN}_complete.txt'.format(
    KW_OUT_DIR = OUT_DIR, KW_DATASET_DIR_NAME = args.input_file, KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_DELETE_TOKEN = DELETE_TOKEN)
print("{}\n".format(graphbolt_output_file))

# Save current iteration commands to shell file.
with open(SHELL_DIR + "/" + args.input_file + SHELL_FILETYPE, "a") as shell_file:
    # Format current moment as a UTC timestamp.
    now = datetime.datetime.utcnow()
    utc_dt = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute, now.second, tzinfo=pytz.utc)
    timestamp = str(utc_dt) + " (UTC)"

    # Add a sequence of the shell-specific comment character for readibility.
    shell_file.write("{} {}  complete execution\n\n".format(SHELL_COMMENT * 5, timestamp))
    
    # Write the command itself.
    shell_file.write('{} > "{}" 2>&1\n\n'.format(graphbolt_run_command, graphbolt_output_file))

# If we are not skipping results and they were invalid/missing, execute.
if (not args.summarized_only) and need_to_run_complete_pagerank and not args.rbo_only:
    print("> Executing complete version...")
    with open(graphbolt_output_file, 'w') as out_file:
        process = start_process(graphbolt_run_command, out_file)
        process_list.append(process.pid)

        print("> Registering children of process {}...".format(process.pid))
        register_grandchildren(process.pid, process_list)
        print("> Finished registering children of process {}.".format(process.pid))
        process.wait()
    print("> Complete PageRank finished.\n")

    upload_zip_files(complete_pagerank_result_path, "graphbolt-storage", "Results", args.skip_upload)
    upload_zip_files(complete_pagerank_stats_path, "graphbolt-storage", "Statistics", args.skip_upload)


else:
    print("> Complete PageRank skipped.\n")


###########################################################################
######################### RUN APPROXIMATE VERSION #########################
###########################################################################



# Execute the summarized version of PageRank for different parameters.
if args.list != None:
    i = 0
    print("Iterating summarized PageRank parameters...")
    while i < len(args.list):
        r = float(args.list[i])
        n = int(args.list[i+1])
        delta = float(args.list[i+2])

        i = i + 3

        print("{}\tGraphBolt parameters - r={}\tn={}\tdelta={}\n".format(SHELL_COMMENT * 5, r, n, delta))

        # This is used as a flag to tell GraphBolt to flush intermediate summary graphs to disk.
        if args.dump_summary:
            SUMMARY_TEXT = "-dump"
        else:
            SUMMARY_TEXT = ""

        # Build command to execute. pt.ulisboa.tecnico.graph.Main
        ### NOTE: the active code below generates a mvn call which launches a separate process for Java (with its own JVM).
        graphbolt_run_command = '''mvn -f ../pom.xml exec:exec -Dexec.executable=java -Dexec.args="-Xmx{KW_MAVEN_HEAP_MEMORY}m -classpath %classpath pt.ulisboa.tecnico.graph.algorithm.pagerank.PageRankMain {KW_FLINK_REMOTE_ADDRESS} {KW_FLINK_REMOTE_PORT} {KW_JOB_MANAGER_WEB_PARAM} {KW_FLINK_JOB_STATS_FLAG} -o '{KW_OUT_BASE}' -cache '{KW_CACHE_BASE}' -damp {KW_DAMPENING_FACTOR:.2f} -iterations {KW_NUM_ITERATIONS} {KW_RBO_RANK_LENGTH} -r {KW_r:.2f} -n {KW_n} -delta {KW_delta:.2f} -web -i '{KW_DATA_DIR}/{KW_DATASET_DIR_NAME}/{KW_DATASET_DIR_NAME}-start.tsv' {KW_KEEP_CACHE} {KW_KEEP_LOGS} -sp {KW_STREAM_PORT} -parallelism {KW_PARALLELISM} {KW_DUMP_SUMMARY_GRAPHS} -temp '{KW_TEMP_DIR}' {KW_PERIODIC_DUMP_STR} {KW_DELETE_FLAG}"'''.format(
            KW_MAVEN_HEAP_MEMORY = args.max_mem, KW_FLINK_REMOTE_ADDRESS = FLINK_REMOTE_ADDRESS, KW_FLINK_REMOTE_PORT = FLINK_REMOTE_PORT, KW_JOB_MANAGER_WEB_PARAM = JOB_MANAGER_WEB_PARAM, KW_FLINK_JOB_STATS_FLAG = FLINK_JOB_STATS_FLAG, KW_OUT_BASE = args.out_dir, KW_CACHE_BASE = CACHE_BASE, 
            KW_DAMPENING_FACTOR = args.dampening, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = SIZE_STR,
            KW_r = r, KW_n = n, KW_delta = delta, KW_DATA_DIR = args.data_dir, 
            KW_DATASET_DIR_NAME = args.input_file, KW_KEEP_CACHE = KEEP_CACHE_TEXT, KW_KEEP_LOGS = KEEP_LOGS_TEXT, KW_STREAM_PORT = STREAM_PORT, KW_PARALLELISM = args.parallelism,
            KW_DUMP_SUMMARY_GRAPHS = SUMMARY_TEXT, KW_TEMP_DIR = TEMP_DIR, KW_PERIODIC_DUMP_STR = PERIODIC_DUMP_STR, KW_DELETE_FLAG = DELETE_FLAG).replace('\\', '/')
        
        print("{}\n".format(graphbolt_run_command))

        # Build path to output directory file.
        graphbolt_output_file = '{KW_OUT_DIR}/{KW_DATASET_DIR_NAME}_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_{KW_r:.2f}_{KW_n}_{KW_delta:.2f}_{KW_DELETE_TOKEN}.txt'.format(
            KW_OUT_DIR = OUT_DIR, KW_DATASET_DIR_NAME = args.input_file, KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening,
            KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size, KW_r = r, KW_n = n, KW_delta = delta, KW_DELETE_TOKEN = DELETE_TOKEN)
        print("{}\n".format(graphbolt_output_file))

        summarized_pagerank_result_path = "{KW_RESULTS_DIR}/{KW_DATASET_DIR_NAME}-start_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_model_{KW_r:.2f}_{KW_n}_{KW_delta:.2f}_{KW_DELETE_TOKEN}".format(
    KW_RESULTS_DIR = RESULTS_DIR, KW_DATASET_DIR_NAME = args.input_file, KW_NUM_ITERATIONS = args.iterations, KW_PARALLELISM = args.parallelism, KW_RBO_RANK_LENGTH = args.size, KW_DAMPENING_FACTOR = args.dampening, KW_r = r, KW_n = n, KW_delta = delta, KW_DELETE_TOKEN = DELETE_TOKEN)

        approx_stats_path = '{KW_STATISTICS_DIR}/{KW_DATASET_DIR_NAME}-start_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_model_{KW_r:.2f}_{KW_n}_{KW_delta:.2f}_{KW_DELETE_TOKEN}'.format(
            KW_STATISTICS_DIR = STATISTICS_DIR, KW_DATASET_DIR_NAME = args.input_file, KW_NUM_ITERATIONS = args.iterations, KW_RBO_RANK_LENGTH = args.size,  KW_PARALLELISM = args.parallelism, KW_DAMPENING_FACTOR = args.dampening, KW_r = r, KW_n = n, KW_delta = delta, KW_DELETE_TOKEN = DELETE_TOKEN)

        # Build summarized PageRank evaluation command (RBO evaluation code).
        graphbolt_eval_command = '{KW_PYTHON_PATH} -m evaluation.rbo.batch_rank_evaluator "{KW_SUMMARIZED_PAGERANK_RESULT_PATH}" "{KW_COMPLETE_PAGERANK_RESULT_PATH}" 0.99'.format(
            KW_PYTHON_PATH = PYTHON_PATH, KW_GRAPHBOLT_DIR = GRAPHBOLT_DIR, KW_SUMMARIZED_PAGERANK_RESULT_PATH = summarized_pagerank_result_path, KW_COMPLETE_PAGERANK_RESULT_PATH = complete_pagerank_result_path)

        # Build path to RBO evaluation file.
        graphbolt_output_eval_path = '{KW_EVAL_DIR}/{KW_DATASET_DIR_NAME}_{KW_NUM_ITERATIONS}_{KW_RBO_RANK_LENGTH}_P{KW_PARALLELISM}_{KW_DAMPENING_FACTOR:.2f}_{KW_r:.2f}_{KW_n}_{KW_delta:.2f}_{KW_DELETE_TOKEN}.csv'.format(
            KW_EVAL_DIR = EVAL_DIR, KW_DATASET_DIR_NAME = args.input_file, KW_NUM_ITERATIONS = args.iterations, KW_PARALLELISM = args.parallelism, KW_RBO_RANK_LENGTH = args.size,  KW_DAMPENING_FACTOR = args.dampening, KW_r = r, KW_n = n, KW_delta = delta, KW_DELETE_TOKEN = DELETE_TOKEN)

        # Save current iteration commands to shell file.
        with open(SHELL_DIR + "/" + args.input_file + SHELL_FILETYPE, "a") as shell_file:
            # Format current moment as a UTC timestamp.
            now = datetime.datetime.utcnow()
            utc_dt = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute, now.second, tzinfo=pytz.utc)
            timestamp = str(utc_dt) + " (UTC)"

            shell_file.write("{} {} r = {}\tn = {}\tdelta = {}\n\n".format(SHELL_COMMENT * 5, timestamp, r, n, delta))
            shell_file.write('{} > "{}" 2>&1\n\n'.format(graphbolt_run_command, graphbolt_output_file))
            shell_file.write('{} > "{}" 2>&1\n\n'.format(graphbolt_eval_command, graphbolt_output_eval_path))

        
        # Skip summarized PageRank version if the directory for a given parameter combination is found.
        need_to_run_this_approx = False
        # Was a results directory found?
        if not os.path.exists(summarized_pagerank_result_path):
            print("> Approximate PageRank results not found:\t{}".format(summarized_pagerank_result_path))
            print("> Need to run summarized PageRank")
            need_to_run_this_approx = True
        else:
            names = os.listdir(summarized_pagerank_result_path)
            paths = [os.path.join(summarized_pagerank_result_path, name) for name in names]
            sizes = [(path, os.stat(path).st_size) for path in paths]

            # Does the result directory have the expected number of files?
            if len(names) != int(args.chunk_count) + 1:
                print("> Expected {} but got {} summarized PageRank results at {}.".format(int(args.chunk_count) + 1, len(names), summarized_pagerank_result_path))
                print("> Need to run summarized PageRank")
                need_to_run_this_approx = True
            else:
                found_zeros = False

                # Do they all have data?
                for [path, sz] in sizes:
                    if os.path.isdir(path):
                        subnames = os.listdir(path)
                        subpaths = [os.path.join(path, name).replace('\\\\', '/') for name in subnames]
                        subsizes = [(s_path, os.stat(s_path).st_size) for s_path in subpaths]

                        if not len(subsizes) == args.parallelism: 
                            found_zeros = True
                            break

                    else:
                        if sz == 0:
                            found_zeros = True
                            break
                if found_zeros:
                    print("> Found summarized PageRank results with value of zero at {}.".format(summarized_pagerank_result_path))
                    print("> Need to run summarized PageRank")
                    need_to_run_this_approx = True


        # If we're invalid/missing and we are not doing RBO only, execute summarized GraphBolt.
        if need_to_run_this_approx and (not args.rbo_only):
            with open(graphbolt_output_file, 'w') as out_file:
                print("> Executing summarized version...")

                print("> Results directory:\t{}".format(summarized_pagerank_result_path))





                print("> Statistics directory:\t{}".format(approx_stats_path))
                print("> Eval file:\t{}".format(graphbolt_output_eval_path))

                process = start_process(graphbolt_run_command, out_file)
                process_list.append(process.pid)

                print("\n> Registering children of process {}...".format(process.pid))
                register_grandchildren(process.pid, process_list)
                print("> Finished registering children of process {}.".format(process.pid))
                process.wait()
                print("> Approximate version r:{KW_r:.2f} n:{KW_n} delta:{KW_delta:.2f} finished.\n".format(KW_r = r, KW_n = n, KW_delta = delta))

                upload_zip_files(summarized_pagerank_result_path, "graphbolt-storage", "Results", args.skip_upload)
                upload_zip_files(approx_stats_path, "graphbolt-storage", "Statistics", args.skip_upload)
        elif args.rbo_only:
            print("> Skipping summarized execution, attempting to generate RBO directly.")
        else:
            print("> Approximate results already found for r:{KW_r:.2f} n:{KW_n} delta:{KW_delta:.2f}.".format(KW_r = r, KW_n = n, KW_delta = delta))

            upload_zip_files(summarized_pagerank_result_path, "graphbolt-storage", "Results", args.skip_upload)
            upload_zip_files(approx_stats_path, "graphbolt-storage", "Statistics", args.skip_upload)

        if not args.summarized_only:

            # Execute RBO evaluation code.
            print("{}".format(graphbolt_eval_command))
            print("{}".format(graphbolt_output_eval_path))

            # If the current paramter combination has an associated summarized (summarized) results directory, calculate RBO.
            if os.path.isdir(summarized_pagerank_result_path):
                with open(graphbolt_output_eval_path, 'w') as rbo_out_file:
                    print("> Calculating RBO of summarized version...\n")


                    batch_rank_evaluator.compute_rbo(summarized_pagerank_result_path, complete_pagerank_result_path, out = rbo_out_file)
            
###########################################################################
############################# CLEANUP ON EXIT #############################
###########################################################################

# Stop all children and grandchildren processes.
#if not args.skip_results:
print("> Stopping execution...")
cleanup(process_list)