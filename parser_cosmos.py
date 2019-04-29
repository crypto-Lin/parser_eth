from pymongo import MongoClient
from pymongo import errors
from bson.code import Code
from bson import ObjectId
import logging

import functools
import time
import requests
import datetime
import json
from multiprocessing import Pool

def add_logger(path):
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)
    
    fh1 = logging.FileHandler(path)
    fh1.setLevel(logging.DEBUG)
    
    # 定义handler的输出格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh1.setFormatter(formatter)
    logger.addHandler(fh1)  
    return logger

def mongoClient(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

def metric(fn):    
    @functools.wraps(fn)
    def wrapper(*args, **kw):
        t0 = time.time()
        back = fn(*args, **kw)
        logger.info('%s executed in %s s' % (fn.__name__, time.time()-t0))
        return back       
    return wrapper

def get_block_results_info(height):
    #print('in get_block_results')
    url = 'http://116.62.210.86:26657/block_results?height=' + str(height)
    try:
        r = requests.get(url).json()        
    except:
        try:
            r = requests.get(url).json()
        except Exception as e:
            logger.error(e)
            
    if r['result']['results']['DeliverTx']==None:
        return {}
    else:
        r['result']['height'] = int(r['result']['height'])
        return r['result']
    
def get_validators(height):
    url = 'http://116.62.210.86:1317/validatorsets/' + str(height)
    try:
        r = requests.get(url).json()        
    except:
        try:
            r = requests.get(url).json()
        except Exception as e:
            logger.error(e)
            
    r['block_height'] = int(r['block_height'])   
    return r

def get_block_info(height):
    url = 'http://116.62.210.86:26657/block?height=' + str(height)
    try:
        r = requests.get(url).json()        
    except:
        try:
            r = requests.get(url).json()
        except Exception as e:
            logger.error(e)
    
    r['result']['block']['header']['height'] = int(r['result']['block']['header']['height'])
    return r['result']
        
def insert_data(get_data_func, height, db):
    #print('i am here!')
    data = get_data_func(height)
    #print(data, get_data_func)
    if data !={}:
        try:
            db.insert_one(data)
            logger.info(height)
        except Exception as e:
            logger.info(get_data_func)
            logger.info(e)
        else:
            pass
        finally:
            pass
@metric
def update_block_info():
    blkReultsDB = mongoClient('localhost','cosmos','block_results')
    validatorsDB = mongoClient('localhost','cosmos','validators')
    blkDB = mongoClient('localhost','cosmos','block')
    
    p = Pool(3)    
    p.apply_async(insert_data, args=(get_block_results_info, 9132, blkReultsDB,))    
    p.apply_async(insert_data, args=(get_validators, 9132, validatorsDB,))    
    p.apply_async(insert_data, args=(get_block_info, 9132, blkDB,))
    
    p.close()
    p.join()

@metric
def update_block_info2(blknums):
    blkReultsDB = mongoClient('localhost','cosmos','block_results')    
    blkDB = mongoClient('localhost','cosmos','block')
    validatorsDB = mongoClient('localhost','cosmos','validators')
    for height in blknums:
        insert_data(get_block_results_info, height, blkReultsDB)
        insert_data(get_block_info, height, blkDB)
        insert_data(get_validators, height, validatorsDB)
    
if __name__ == '__main__':
    
    logger = add_logger('./cosmos.log')
    
    start = 1
    end = 70575
    lens = 200
    blks = [(i, min(i+lens, end)) for i in range(start, end , lens)]
    for i in range(len(blks)):
        rang = blks[len(blks)-i-1]
        blknums = [j for j in range(rang[0], rang[1])]    
        #update_block_info(blknums)
        update_block_info2(blknums)
        if i%100 == 0:
            print('{} blocks have been processed.'.format(i*100))