# Extract all tx info from eth_getTransactionReceipt method 

import eth_parser_tool as tl
import logging

logger = logging.getLogger('logger')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('./parser_by_receipt.log')
fh.setLevel(logging.DEBUG)

# define the output format of handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)

blk_db = tl.Mongoclient('localhost', 'ethereum', 'block')
tx_db = tl.Mongoclient('localhost', 'ethereum', 'tx')

latest_blk = tl.get_block_bynumber()
start_blk = 46147

for i in range(start_blk, latest_blk+1):
	blk = start_blk + latest_blk - i
	info = tl.get_block_bynumber(blk)['result']

	txs = info['transactions']
	del info['transactions']
	info['tx_num'] = len(txs)
	info['_id'] = int(info['number'], 16)
	try:
		blk_db.insert_one(info)
	except Exception as e:
		logger.info(blk)
		logger.error(e)

	for tx in txs:
		full_tx = tl.get_tx_info(tx['hash'])['result']
		try:
			tx_db.insert_one(full_tx)
		except Exception as e:
			logger.info(blk)
			logger.error(e)

	logger.info(blk)

