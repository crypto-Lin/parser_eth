import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from functools import reduce

def extract_table(url, addr = ''): # extract both tx table and internaltx table
    r = requests.get(url).text
    soup = BeautifulSoup(r, 'lxml')
    tables = soup.find_all('tbody')
    
    txs = tables[0].find_all('tr')
    tmp = []
    for tr in txs:
        tmp.append([td.get_text() for td in tr])
        
    try:
        internaltx = tables[1].find_all('tr')
    except:
        return {'tx':tmp, 'internaltx':[]}

    tmp2 =[]
    for tr in internaltx:
        tmp2.append([td.get_text() for td in tr])
    
    return {'tx':tmp, 'internaltx':tmp2}

def extract_table2(url, addr=''): # extract long tx table and internaltx table
    tmp = []
    for page in range(1,4):
        url_tx = 'https://etherscan.io/txs?a='+addr+'&ps=100&p='+str(page)
        try:
            tmp = tmp + extract_table(url_tx)['tx']
        except:
            continue
    tmp2 = extract_table(url)['internaltx']   

    return {'tx':tmp, 'internaltx':tmp2}

def calculate_total_in(df, f1): # f1 -> extract_table or extract_table2
    result = []
    for i in df.index:
        name = df.loc[i, 'name'] 
        addr = i.replace(' ','')
        url = 'https://etherscan.io/address/'+ addr
        tot_in = 0
        print(addr)             
        table = f1(url, addr)
        if len(table['tx'])>0:
            df1 = pd.DataFrame(table['tx'], columns=['txhash','height','time','from','direction','to','value','txfee'])
            df1 = df1[df1['direction'] == '\xa0IN\xa0']
            tot_in = reduce(lambda x, y:x+y, [float(''.join(ele.split(' ')[0].split(','))) for ele in df1['value'].tolist()])
        if len(table['internaltx']) >0:
            df2 = pd.DataFrame(table['internaltx'], columns=['txhash','height','time','from','unknown','to','value'])
            df2 = df2[(df2['to'] == name) | (df2['to'] == addr)]
            if len(df2)>0:
                tot_in = reduce(lambda x, y:x+y, [float(''.join(ele.split(' ')[0].split(','))) for ele in df2['value'].tolist()]) + tot_in                    
            
        result.append((addr, name, tot_in))
    return result

if __name__ == '__main__':
    url = 'https://etherscan.io/accounts?l=Donate'
    donate = pd.DataFrame(extract_table(url)['tx'], columns=['address','name','balance','txCount'])
    donate['txs'] = [int(''.join(ele.split(','))) for ele in donate['txCount'].tolist()]
    donate = donate.set_index('address')

    first_class = donate[donate['txs'] <= 25]
    result = calculate_total_in(first_class, extract_table)

    third_class = ['0xd556caf704e39fc728058557a113b226207d2212','0xdecaf9cd2367cdbb726e904cd6397edfcae6068d','0xfb6916095ca1df60bb79ce92ce3ea74c37c5d359']
    second_class = donate[(donate['txs'] > 25) & (~donate.index.isin(third_class))]
    result = result + calculate_total_in(second_class, extract_table2)

    for addr in third_class:    
        tot_in = 0
        df_tx = pd.read_csv('/Users/admin/Desktop/'+ addr+'.csv')
        df_internaltx = pd.read_csv('/Users/admin/Desktop/'+ addr+'_internal.csv')
        
        df_tx = df_tx[df_tx['Status'].isna()]
        tot_in = df_tx['Value_IN(ETH)'].sum()
       
        df_internaltx = df_internaltx[df_internaltx['Status']==0]
        tot_in = df_internaltx['Value_IN(ETH)'].sum() + tot_in
        result.append((addr, donate.loc[addr,'name'] ,tot_in))

    df = pd.DataFrame(result, columns=['address','name','donation'])   
    print(df)

