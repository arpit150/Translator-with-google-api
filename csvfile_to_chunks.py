import csv
import sys
import requests as r
import urllib.parse 
import json
from time import time
from func import check_type,insert_db,createTable,arrayToString,type_count,alphanum_break,digit_convert,special_break,replace_,convertTohindi
import psycopg2
import html,time
import pandas as pd
import os


delimiter = ';'


tableName = 'bengali'
lang_code ='bn'
limit_offset = 40
conn = psycopg2.connect("dbname='python_trans' user='postgres' host='localhost' password='postgres'")
cur = conn.cursor()

def checkCount(tableName):
  cur.execute(f"select count(id) from data.{tableName} where output=''")
  # Fetch result
  record = cur.fetchone()
  return record[0]

def convert(lang,word):
    dict_ ={} 
    word_convert = urllib.parse.quote(word)
    try:
      url = f'https://inputtools.google.com/request?text={word_convert}&itc={lang}-t-i0-und&num=13&cp=0&cs=1&ie=utf-8&oe=utf-8&app=demopage'
      print(url)
      data = r.get(url)
      aa = json.loads(data.content)
      print(aa)
    except :
      print('an error occurred!!!! wait for a min')
      time.sleep(60)
    try:
      for i2 in aa[1]:
        try:
          dict_[html.unescape(i2[0])]=html.unescape(i2[1][0])
        except:
          dict_[html.unescape(i2[0])]='N/A'
    except:
      # sys.exit()
      pass
    

    return dict_



def addCol(path:str):
  df = pd.read_csv(path,sep=delimiter)
  df.rename(columns={'ENG_NME':'input'},inplace=True)
  df['output']=''
  df['remark']=''
  os.remove(path)

  df.to_csv(path,index=False,sep=delimiter)


createTable(conn,tableName)

filename = r"D:\arpit\convert\archive\Indic UnMatch Data For Translate\UnMatch_Regional_Word_BN.csv"
addCol(filename)
with open(filename, 'rb') as f:
  next(f) # Skip the header row.
  print(f)
  cur.copy_from(f, 'data.'+str(tableName),sep=delimiter)
  conn.commit()



# counts
while(checkCount(tableName)>1):
  count_normal =type_count(tableName,'normal')
  count_abbr =type_count(tableName,'abbr')
  count_alphanum =type_count(tableName,'alphanum')
  count_special =type_count(tableName,'special')


  print('converting normal')
  endindex = int(count_normal)+limit_offset
  for i in range(0,endindex,limit_offset):
    data_to_send = arrayToString(conn,i,tableName,'normal',limit_offset,',')
    coverted_data = convert(lang_code,data_to_send)
    # if(coverted_data['error']):
    #   continue
    for keys in coverted_data.keys():
      values = coverted_data[keys]
      update_query = F"""UPDATE data.{tableName} SET  output=%s where input = %s"""
      cur.execute(update_query,(values,keys))
      print(keys)
      conn.commit()

  # abbr

  print('converting abbr')

  for i in range(0,int(count_abbr),limit_offset):
    data_to_send = arrayToString(conn,i,tableName,'abbr',limit_offset,',')
    
    split_list = data_to_send.split(',')
    split_data = ','.join(['.'.join(list(i)) for i in split_list])
    coverted_data = convert(lang_code,split_data)
    keyArr = coverted_data.keys()
    for keys in keyArr:
      
      values = coverted_data[keys]
      keys_=keys.replace('.','')
      update_query = F"""UPDATE data.{tableName} SET  output='{values}' where input ='{keys_}'"""
      try:
        cur.execute(update_query)
        conn.commit()
      except psycopg2.OperationalError as e:
          print('Unable to insert!\n{0}').format(e)
          sys.exit(1)
      print(keys_)

      conn.commit()





  # special
  print('converting special')

  print(count_special)
  for i in range(0,int(count_special),limit_offset):
    # print(i)
    data_to_send = arrayToString(conn,i,tableName,'special',limit_offset,'π').replace(',','δ')
    # print(data_to_send)
    coverted_data = convert(lang_code,data_to_send.replace('π',','))
    print(coverted_data)
    # sys.exit()
    for keys in coverted_data.keys():
      values = digit_convert(coverted_data[keys],lang_code)
      update_query = F"""UPDATE data.{tableName} SET  output=%s where input = %s"""
      cur.execute(update_query,(values,keys))
      print(keys)

      conn.commit()


  # alphanum
  print('converting alphanum')
  # print(count_alphanum)

  for i in range(0,int(count_alphanum),limit_offset):
    print(i)
    data_to_send = arrayToString(conn,i,tableName,'alphanum',limit_offset,',')
    dataArr = map(alphanum_break,data_to_send.split(','))
    data_to_send =','.join(list(dataArr))
    coverted_data = convert(lang_code,data_to_send)
    for keys in coverted_data.keys():
      values = digit_convert(coverted_data[keys],lang_code)
      keys=keys.replace('.','')
      update_query = F"""UPDATE data.{tableName} SET  output=%s where input = %s"""
      cur.execute(update_query,(values,keys))
      print(keys)

      conn.commit()
  print('done')


if lang_code =='hi':
  from sqlalchemy import create_engine
  import pandas as pd
  engine = create_engine('postgresql://postgres:postgres@localhost:5432/python_trans')
  df = pd.read_sql_table(tableName,con=engine,schema='data')
  print(df)
  df['output'] = df.apply(lambda x :x['output'].replace('ा','अ'),axis=1) 
  temp = pd.DataFrame()
  for i in df.index:
      _id = df['id'][i]
      print(_id)
      _input = df['input'][i]
      new_output = replace_(df['output'][i],'ा','अ')
      _remark = df['remark'][i]
      temp = temp.append({'id':_id,'input':_input,'output':new_output,'remark':_remark},ignore_index=True)
  print('table update')
  print(temp)
  print(df)
  df.to_csv(f'{tableName}.csv',index=False)

  # if lang_code =='hi':
  #   from sqlalchemy import create_engine
  #   import pandas as pd
  #   engine = create_engine('postgresql://postgres:postgres@localhost:5432/python_trans')
  #   df = pd.read_sql_table(tableName,con=engine,schema='data')
  #   print(df)
  #   df['output'] = df['output'].apply(convertTohindi)
  #   df.to_csv(f'E:\convert_07112020\{tableName}.csv',index=False)

conn.close()