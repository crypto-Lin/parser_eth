import pandas as pd
import json
import requests
import datetime

from pymongo import MongoClient
from pymongo import errors

from bs4 import BeautifulSoup
from lxml import etree
import logging

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

def get_by3(url):
    try:
        html = requests.get(url)
    except Exception as e:
        try:
            html = requests.get(url)
        except Exception as e:
            try:
                html = requests.get(url)
            except Exception as e:
                logger.error(e)
    return html.text

if __name__ == '__main__':

    url_1st = 'https://etherscan.io/labelcloud'
    soup_1st = BeautifulSoup(get_by3(url_1st), features='lxml')
    label_list = []
    
    logger = add_logger('./update_label.log')
    cllc_label = mongoClient('localhost','parity','eth_label')
    
    for container in soup_1st.find_all('div', {'class': 'col-sm-6 col-sm-4 col-md-3 p-2 secondary-container'}):
        link_tag = container.find_all('a')
        
        for link in link_tag:
            
            url_2nd = 'https://etherscan.io' + link.get('href')
            print(url_2nd)
            
            # find the total pages num
            try:
                script = soup_2nd.find('ul', {'class':'pagination pagination-sm mb-0'}).find_all('strong')[1]
                page_num = int(script.get_text())
            except:
                page_num = 1
                
            # 3 cases: accounts, tokens, transactions
            if 'accounts' in url_2nd:
                print('accounts')
                for page_num in range(1, page_num + 1):
                    if page_num == 1:
                        url_3rd = url_2nd
                    else:
                        url_3rd = url_2nd.split('?')[0] + '/' + str(page_num) + '?' + url_2nd.split('?')[1]

                    try:
                        soup_3rd = BeautifulSoup(get_by3(url_3rd), features='lxml')
                    except Exception as e:
                        logger.error('Error 1: %s', e)
                        logger.info(url_3rd)
                    
                    table = soup_3rd.tbody.find_all('tr')
                    for row in table:
                        if row.i != None:
                            isContract = True
                        else:
                            isContract = False
                        pill = row.find_all('td')
                        addr_info = [ele.get_text() for ele in pill]

                        try:                            
                            temp = {}
                            temp['address'] = addr_info[0].replace(' ','')
                            temp['name'] = addr_info[1]
                            temp['label'] = link.get('href').split('=')[1]
                            temp['isContract'] = isContract
                            temp['_id'] = addr_info[0].replace(' ','') + '_' + temp['label']
                        except Exception as e:
                            logger.error('Error 2: %s', e)
                            logger.info(url_3rd)

                        try:
                            cllc_label.insert_one(temp)
                        except Exception as e:
                            logger.error(e, exe_info=True)

            if 'tokens' in url_2nd:
                print('tokens')
            
                for page_num in range(1, page_num + 1):
                    url_3rd = url_2nd + '&p=' + str(page_num)
                    print(url_3rd)

                    try:
                        soup_3rd = BeautifulSoup(get_by3(url_3rd), features='lxml')
                    except Exception as e:
                        logger.error('Error 3: %s',e)
                        continue

                    table = soup_3rd.tbody.find_all('tr')

                    for row in table:
                        pill = row.find_all('td')
                        addr_info = [ele.get_text() for ele in pill]
                        temp = {}
                        address = addr_info[0].replace(' ','')
                        
                        if 'Spam Token' in url_3rd:
                            temp = {'address': address, 'label': link.get('href').split('=')[1], 'isContract': True}
                            temp['_id'] = address + '_' + temp['label']
                        else:
                            temp = {'address': address, 'name': addr_info[1], 'symbol': addr_info[2],
                                    'website': addr_info[3].replace(' ',''), 'label': link.get('href').split('=')[1], 'isContract': True}
                            temp['_id'] = address + '_' + temp['label']

                        try:
                            cllc_label.insert_one(temp)
                        except Exception as e:
                            logger.error(e)

            if 'txs' in url_2nd:
                soup_3rd = BeautifulSoup(get_by3(url_2nd), features='lxml')
                table = soup_3rd.tbody.find_all('tr')
                for row in table:
                    addr_info = [td.get_text() for td in tr]
                    pass
                
                logger.info('label is tx:')
                logger.info(url_2nd)
                