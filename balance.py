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

def get_balance(addr):
    data = {
        "jsonrpc": "2.0",
        "method": "eth_getBalance",
        "params": [addr],
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
    return int(r['result'], 16)* 1e-18

def insert_balance(addr):
    val = get_balance(addr)
    test.update_one({'_id':addr},{'$set':{'bal': val}})

def metric(fn):    
    @functools.wraps(fn)
    def wrapper(*args, **kw):
        t0 = time.time()
        back = fn(*args, **kw)
        print('%s executed in %s s' % (fn.__name__, time.time()-t0))
        return back       
    return wrapper

@metric
def update_balances(addrs):
    p = Pool(10)
    for addr in addrs:
        p.apply_async(insert_balance, args=(addr,))
    p.close()
    p.join()

if __name__ == '__main__':
    logger = add_logger('./parity_log/bal_test.log')

    url = 'http://47.96.30.45:8549'
    bal_db = mongoClient('localhost', 'parity', 'bal_test')
    addrs = []
    for doc in bal_db.find():
        addrs.append(doc['_id'])
        if len(addrs) == 500:
            print('round new')
            update_balances(addrs)
            addrs = []



