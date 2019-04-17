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

def get_tx_info(txhash):
    data = {
            "jsonrpc":"2.0",
            "method":"eth_getTransactionReceipt",
            "params":[txhash],
            "id" : 1
            }
    headers = {'Content-Type':'application/json'}
    
    try:
        r = requests.post(url, headers = headers, data= json.dumps(data)).json()
    except:
        try:
            r = requests.post(url, headers = headers, data= json.dumps(data)).json()
        except:
            try:
                r = requests.post(url, headers = headers, data= json.dumps(data)).json()
            except Exception as e:
                logger.error (e)
    #print(r['result'])
    return r

def get_current_blknum():
    data = {
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }
    headers = {'Content-Type': 'application/json'}

    try:
        r = requests.post(url, headers=headers, data=json.dumps(data)).json()
    except:
        try:
            r = requests.post(url, headers=headers, data=json.dumps(data)).json()
        except:
            try:
                r = requests.post(url, headers=headers, data=json.dumps(data)).json()
            except Exception as e:
                logger.error(e)
    return int(r['result'], 16)

def get_block_bynumber(blknum):
    #print('get_block_bynumber is called!')
    blknum = hex(blknum)

    data = {
            "jsonrpc":"2.0",
            "method":"eth_getBlockByNumber",
            "params":[blknum, True],
            "id" : 1
            }
    headers = {'Content-Type':'application/json'}
    
    try:
        r = requests.post(url, headers = headers, data= json.dumps(data)).json()
    except:
        try:
            r = requests.post(url, headers = headers, data= json.dumps(data)).json()
        except:
            try:
                r = requests.post(url, headers = headers, data= json.dumps(data)).json()
            except Exception as e:
                logger.error(e)    
    return r

def insert_balance(addr):
    val = get_balance(addr)
    test.update_one({'_id':addr},{'$set':{'bal': val}})

def metric(fn):    
    @functools.wraps(fn)
    def wrapper(*args, **kw):
        t0 = time.time()
        back = fn(*args, **kw)
        logger.info('%s executed in %s s' % (fn.__name__, time.time()-t0))
        return back       
    return wrapper

@metric
def update_balances(addrs):
    p = Pool(10)
    for addr in addrs:
        p.apply_async(insert_balance, args=(addr,))
    p.close()
    p.join()

def insert_tx(txhashs):
    tx_db = mongoClient('localhost', 'ethereum', 'tx')
    tx_infos = []
    for txhash in txhashs:
        full_tx = get_tx_info(txhash)['result']
        full_tx['blockNumber'] = int(full_tx['blockNumber'],16)
        tx_infos.append(full_tx) 
        
    if len(tx_infos) == 0:
        pass
        #logger.info('empty block')
    else:
        try:
            tx_db.insert_many(tx_infos)
        except Exception as e:
            logger.error(e)
        
def insert_block(blknum):
    blk_db = mongoClient('localhost', 'ethereum', 'block')
    info = get_block_bynumber(blknum)['result']       
    txhashs = [ele['hash'] for ele in info['transactions']]
    del info['transactions']
    info['tx_num'] = len(txhashs)
    info['number'] = int(info['number'], 16)
    
    try:
        blk_db.insert_one(info)
    except Exception as e:    
        logger.info(blknum)
        logger.error(e)
                
    insert_tx(txhashs)
    
@metric 
def update_tx_info(blknums):
    p = Pool(len(blknums))
    for blknum in blknums:
        p.apply_async(insert_block, args=(blknum,))
    p.close()
    p.join()
    
    logger.info(blknums)
    
if __name__ == '__main__':
    
    logger = add_logger('./test.log')
    url = 'http://47.96.228.84:8545'
    
    start = 4000000
    end = get_current_blknum() +1
    lens = 100
    blks = [(i, min(i+lens, end)) for i in range(start, end , lens)]
    for i in range(len(blks)):
        rang = blks[len(blks)-i-1]
        blknums = [j for j in range(rang[0], rang[1])]    
        update_tx_info(blknums)
        if i%100 == 0:
            print('{} blocks have been processed.'.format(i*100))
        