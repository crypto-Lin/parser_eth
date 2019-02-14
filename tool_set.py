# In this project, I am trying to parser ethereum blockchain data as much as possible. 
# web3.py is used to interact with ethereum full node. 
# All the data are stored in Postgresql. 
# I am gonna to build 3 tables in total. One is for block information, second for internel 
# tx infos from parity and the third for basic infos from ethereum log system. 

#--------------------------------interact with postgresql--------------------------------#

def Postgresql_client():
	pass



#--------------------------------interact with parityClient------------------------------#

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

#-------------------------interact with node using web3 frame------------------------------#






