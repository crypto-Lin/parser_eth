from pymongo import MongoClient
from pymongo import errors
from bson.code import Code
from bson import ObjectId
import logging

import functools
import time

def mongoClient(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

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

def metric(fn):    
    @functools.wraps(fn)
    def wrapper(*args, **kw):
        t0 = time.time()
        back = fn(*args, **kw)
        logger.info('%s executed in %s s' % (fn.__name__, time.time()-t0))
        return back       
    return wrapper

@metric
def mapReduce_lastHeight(start, end, db):
    mapper = Code("""
        function(){emit(this.fr, this.blk);}""")
    reducer = Code("""
        function(key, values){return Math.max.apply(null, values);}""")

    results = db.map_reduce(mapper, reducer, "myresult", query={'blk': {'$gte': start, '$lte': end}})
    for doc in results.find():
        balanceDB.save({'_id': doc['_id'], 'height': doc['value']})
        results.delete_one(doc)
    
    mapper = Code("""
        function(){emit(this.to, this.blk);}""")
    results = db.map_reduce(mapper, reducer, "myresult", query={'blk': {'$gte': start, '$lte': end}})
    for doc in results.find():
        balanceDB.save({'_id': doc['_id'], 'height': doc['value']})
        results.delete_one(doc)


def update_height(start, end, db, updateDB):
    mapper = Code("""
        function(){emit(this.fr, this.blk);}""")
    reducer = Code("""
        function(key, values){return Math.max.apply(null, values);}""")

    results = db.map_reduce(mapper, reducer, "myresult", query={'blk': {'$gte': start, '$lte': end}})
    for doc in results.find():
        try:
            ID = [updateDB.find({'addr': doc['_id']})][0]['_id']
        except:
            updateDB.insert_one({'addr': doc['_id'], 'height': doc['value']})
            continue
        updateDB.update({'_id':ID},{'$set': {'height': doc['value']}})
        
        results.delete_one(doc)
        
    mapper = Code("""
        function(){emit(this.to, this.blk);}""")
    
    results = db.map_reduce(mapper, reducer, "myresult", query={'blk': {'$gte': start, '$lte': end}})
    for doc in results.find():
        try:
            ID = [updateDB.find({'addr': doc['_id']})][0]['_id']
        except:
            updateDB.insert_one({'addr': doc['_id'], 'height': doc['value']})
            continue
        updateDB.update({'_id':ID},{'$set': {'height': doc['value']}})
        
        results.delete_one(doc)

if __name__ == '__main__':

    logger = add_logger('./parity_log/balance.log')
    balanceDB = mongoClient('localhost','parity','balanceDB')
    db4 = mongoClient('localhost','parity','tx4')
    db5 = mongoClient('localhost','parity','tx5')
    db6 = mongoClient('localhost','parity','tx6')
    
#     for i in range(5000000,6000000,1000):
#         mapReduce_lastHeight(i, i+999, db5)
#         logger.info('%s completed', (i+1000-5000000)/10000)
    
    for i in range(6000000,7000000,1000):
        mapReduce_lastHeight(i, i+999, db6)
        logger.info('%f completed', (i+1000-6000000)/10000)



