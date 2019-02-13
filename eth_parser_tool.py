
# coding: utf-8

# #### All functions used to parser the ETH are aggregated in this .py file
# 

# In[1]:


#---------------------interact with mongodb-------------------------#

from pymongo import MongoClient
from pymongo import errors

def mongoClient(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection


# In[8]:


#-------------------interact with ETH JsonRPC-------------------------#
from datetime import datetime
import requests
import datetime
import json
import os

url = 'http://47.96.30.45:8545'

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

def get_latest_eth_balance(address):    
    data = {
            "jsonrpc":"2.0",
            "method":"eth_getBalance",
            "params":[address, "latest"],
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
    return int(r['result'], 16) * 1e-18

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
    return r

def get_eth_code(addr):    
    data = {
            "jsonrpc":"2.0",
            "method":"eth_getCode",
            "params":[addr, 'latest'],
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
    return r

def Iscontract(addr):
    try:
        r = get_eth_code(addr)
        r = r['result']
    except Exception as e:
        logger.error(e)
        return False
    
    if r != '0x':
        return True
    else:
        return False
    
def get_block_bynumber(blknum):
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

def blocknum_to_time(blknum):
    data = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(blknum), True],
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
    try:
        timestamp = int(r['result']['timestamp'], 16)
        time = datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(e)

    return time


# In[4]:


#-------------------interact with parityClient-------------------------#

def trace_replay_block_tx(blknum):
    data = {
        "jsonrpc": "2.0",
        "method": "trace_replayBlockTransactions",
        "params": [hex(blknum), ["trace"]],
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
    return r

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
    except:
        try:
            r = requests.post(url, headers=headers, data=json.dumps(data)).json()
        except:
            try:
                r = requests.post(url, headers=headers, data=json.dumps(data)).json()
            except Exception as e:
                logger.error(e)
    return r


# In[ ]:


#-----------------------interact with etherscan.io---------------------------------#

ABI_ENDPOINT = 'https://api.etherscan.io/api?module=contract&action=getabi&address='

#abi = mongoClient('localhost:27017', 'eth_warning', 'contract_abi')

def fetch_abi(addr):

    response = requests.get('%s%s'%(ABI_ENDPOINT, addr))
    response_json = response.json()
    abi = response_json['result']
    
#     abi_json = json.loads(response_json['result'])
#     result = json.dumps({"abi":abi_json}, sort_keys=True)
    
    return abi
    
