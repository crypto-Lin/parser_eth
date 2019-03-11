import json
import requests
import datetime
import copy
import time
import sys
import logging
import os

from pymongo import MongoClient
from pymongo import errors

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

def trace_replay_tx(txhash):
    data = {
        "jsonrpc": "2.0",
        "method": "trace_replayTransaction",
        "params": [txhash, ["trace"]],
        "id": 1
    }
    headers = {'Content-Type': 'application/json'}

    try:
        r = requests.post(url, headers=headers, data=json.dumps(data)).json()
    except Exception as e:
        try:
            r = requests.post(url, headers=headers, data=json.dumps(data)).json()
        except Exception as e:
            try:
                r = requests.post(url, headers=headers, data=json.dumps(data)).json()
            except Exception as e :
                logger.error(e)
    return r

def TxInfo_by_ParityTrace(txhash, blknum):
    tx_cluster = []
    
    tx = trace_replay_tx(txhash)
    try:
        trace = tx['result']['trace']
    except Exception as e :
        logger.info(e)
        logger.info(txhash)
        logger.info(tx)
    if 'error' in trace[0]:
        return []
        
    num_layer = 0
    for action in trace:
        try:
            transfer_value = int(action['action']['value'], 16) * 1e-18
        except Exception as e :
            continue

        if transfer_value > 0:
            if action['type'] == 'call':
                if action['action']['callType'] == 'delegatecall':
                    continue

            temp = {}
            if action['type'] == 'create':
                try:
                    temp['to'] = action['result']['address']
                except Exception as e:
                    logger.info(e)
                    logger.info(txhash)
                    logger.info(tx)
            else:
                try:
                    temp['to'] = action['action']['to']
                except Exception as e:
                    logger.info(e)
            try:
                temp['fr'] = action['action']['from']
                temp['val'] = action['action']['value']
                temp['hash'] = txhash
                temp['blk'] = blknum
                temp['ord'] = num_layer
                
                tx_cluster.append(temp)
            except Exception as e:
                logger.info(e)
                continue
            
        num_layer += 1
            
    return tx_cluster
def parser_xu_data(patch_ls, err_file):
    # status variables
    black_hash = '0x'
    temp_hash = '0x1'
    flag = 0  # note the initial status
    num_of_son = 0
    num_of_hash = 0
    tx_cluster = []
    
    for i in patch_ls:
        log = str(i) + '.log'
        with open(FilePath+log) as f:
            print(FilePath+log)
            logger.info('=====================================================================')
            logger.info(FilePath+log)

            data = f.readlines()
            for line in data:
                dict_ls = []
                word = line.strip()
                
                if len(word) == 0: # empty line
                    continue
                    
                try: 
                    dict_ls.append(json.loads(word.lower()))
                except:
                    try:
                        dict_ls = word.lower().replace('}{', '}?{').split('?')
                        dict_ls = [json.loads(item) for item in dict_ls]
                        
                    except Exception as e:
                        logger.error(e)
                        logger.error(word.lower())
                    
                for word_dict in dict_ls:
                        
                    txhash = word_dict['hash'] + word_dict['parenthash']
                    
                    if txhash == '':
                        continue 

                    if flag == 0:
                        temp_hash = txhash
                        flag = 1

                    try:
                        if word_dict['from'] == word_dict['to']:
                            continue 
                    except:
                        logger.info(word_dict)      

                    if word_dict['err'] != '':
                        black_hash = word_dict['hash']
                        err_file.write(json.dumps(word_dict) + '\n')
                        continue

                    if word_dict['from'] == '':
                        continue     

                    if txhash != temp_hash:    

                        try:
                            if (temp_hash != black_hash) and (num_of_hash == 1):                            
                                if len(tx_cluster) > 0:
                                    cllc_tx.insert_many(tx_cluster)  

                            if num_of_hash > 1:
                                blknum = tx_cluster[0]['blk']
                                tx_cluster = TxInfo_by_ParityTrace(temp_hash, blknum)

                                if len(tx_cluster) > 0:
                                    cllc_tx.insert_many(tx_cluster) 

    #                         if temp_hash == black_hash:
    #                             if len(tx_cluster) > 0:
    #                                 for tx in tx_cluster:
    #                                     err_file.write(json.dumps(tx))

                        except Exception as e:
                            logger.info('mongo')
                            logger.error(e)

                        tx_cluster = []
                        num_of_son = 0
                        num_of_hash = 0                    
                        temp_hash = txhash

                    temp = {}
                    temp['val'] = word_dict['value']
                    if int(word_dict['value']) > 0:
                        temp['fr'] = word_dict['from']
                        temp['to'] = word_dict['to']
                        temp['blk'] = int(word_dict['block'])

                        if len(word_dict['hash']) > 2:
                            temp['hash'] = word_dict['hash']
                            temp['ord'] = 0
                            num_of_hash += 1 
                            tx_cluster.append(temp)

                        elif len(word_dict['parenthash']) > 2:
                            temp['hash'] = word_dict['parenthash']
                            num_of_son += 1
                            temp['ord'] = num_of_son
                            tx_cluster.append(temp)

                        else:    
                            logger.error('can not find the tx hash!')
if __name__ == '__main__':
    url = 'http://47.96.30.45:8545'

    uri = 'localhost'
    client = MongoClient(uri, 27017)
    db = client['parity']
    cllc_tx = db['xu']

    FilePath = '/data/gopath/src/github.com/ethereum/go-ethereum/build/bin/hack_data/'
    logger = add_logger('./xu_490_499.log')

    patch_ls = [i for i in range(490, 499+1)]
    err_file = open('./err_log_490_499.txt', 'w')
    parser_xu_data(patch_ls, err_file)







