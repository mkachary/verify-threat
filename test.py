import itertools
import ndjson
import json
import encryption
import os
import multiprocessing
from functools import partial
# from joblib import Parallel, delayed
# import mkl
import warnings
# warnings.filterwarnings("ignore", category=UserWarning)
import time


# # Multi threading Block
# def anonymiser(line, enc, wr):
#     # print(line, '\n\n')
#     # with open(conf['output_path'], 'a+') as wr:
#     k = ndjson.loads(line)
#     # print(k)
#     k[0]['_source']['data']['username'] = (enc.encrypt(k[0]['_source']['data']['username'])).decode('utf8')
#     ndjson.dump(k, wr)
#     wr.write('\n')

# def multiprocessing_stream(enc, input_file, output_file):
#     from multiprocessing.dummy import Pool as ThreadPool
#     pool = ThreadPool(8)
#     # pool = multiprocessing.Pool(processes= os.cpu_count())
#     if os.path.isfile(output_file):
#         os.remove(output_file)
#     else:
#         print('The output file doesnt exist')
#     # print(enc)
#     with open(output_file, 'w+') as wr:
#         with open(input_file, 'r') as f:
#             pool.map(partial(anonymiser, enc=enc, wr=wr), f)
#     pool.close()
#     pool.join()


# # Parallel Processing Block
def read_as_stream(enc, input_file, output_file):
    with open(output_file, 'w+') as wr:
        with open(input_file, 'r') as f:
            for line in f:
                k = ndjson.loads(line)
                # print(k)
                k[0]['_source']['data']['username'] = (enc.encrypt(k[0]['_source']['data']['username'])).decode('utf8')
                ndjson.dump(k, wr)
                wr.write('\n')


def anonymiser(line, enc):
    k = json.loads(line)
    # print(line)
    k['_source']['data']['username'] = (enc.encrypt(k['_source']['data']['username'])).decode('utf8')
    return k

def multiprocessing_stream(enc, input_file, output_file, n_lines):
    pool = multiprocessing.Pool(processes=os.cpu_count())
    if os.path.isfile(output_file):
        os.remove(output_file)
    else:
        print('The output file doesnt exist')
    repo = []
    with open(output_file, 'w+') as wr:
        with open(input_file, 'r') as f:
            for i, line in enumerate(f,1):
                repo.append(line)
                if(i%n_lines==0):
                    # print(repo)
                    res = pool.map(partial(anonymiser, enc = enc), repo)
                    repo = []
                    ndjson.dump(res, wr)
                    wr.write("\n")
        res = pool.map(partial(anonymiser, enc = enc), repo)
        ndjson.dump(res, wr)



if(__name__ =="__main__"):
    with open('configuration/config.json', 'r') as configuration:
        conf = json.load(configuration)
    enc = encryption.Encryption(key = conf['Encryption_key'])
    input_file = conf['input_path']
    output_file = conf['output_path']
    max_instances = conf['Max_instances_to_process_at_once']
    start = time.time()
    multiprocessing_stream(enc, input_file, output_file, max_instances)
    print("time taken for multiprocessing",time.time()-start)

    # start = time.time()
    # read_as_stream(enc, input_file, output_file)
    # print("Time taken for single for loop",time.time()-start)