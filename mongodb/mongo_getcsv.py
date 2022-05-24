from dataclasses import dataclass
from pymongo import mongo_client
import sys
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from config.func import *
from datetime import datetime
import csv
# 시작 시간
start_time = time.time()
#ssh 접속 정보 가져오기 및 aws ec2 접속
ssh_conn_info = get_ssh_connect_information()
server = ssh_conn(ssh_conn_info[0],'ec2-user',ssh_conn_info[1],'127.0.0.1',27017)
if server is None:
    print("Error connecting to the AWS Server")
else:
    print("AWS Server connecting established")
server.start()

# 몽고db 접속 정보 가져오기 및 연결
mongo_conn_info = get_mongo_connect_information()
conn = mongo_client.MongoClient(f"mongodb://{mongo_conn_info[2]}:{mongo_conn_info[3]}@{mongo_conn_info[0]}:{mongo_conn_info[1]}")

# 데이터베이스 연결 후 작업 실행
db = conn.mydb
print(db)
mongo_collection = db.sample
print(mongo_collection)

docs = mongo_collection.find(batch_size=3000)
data = []
for doc in docs:
    event_id = str(doc.get("event_id",-1))
    event_timestamp = doc.get("event_timestamp", None)
    event_name = doc.get("event_name", None)
    
    tmp_data =[event_id,event_name,event_timestamp]
    data.append(tmp_data)

local_file = '/Users/kimhyojin/Desktop/pipeline/data/mongo_export.csv'
with open(local_file, 'w') as fp:
    csvw = csv.writer(fp, delimiter='|')
    csvw.writerows(data)

fp.close()
server.close()
conn.close()

# csv 파일 저장을 위한 aws s3 접속 정보 가져오기 및 s3 접속
s3_conn_info = get_s3_connect_information()
s3 = s3_conn('s3', s3_conn_info[0], s3_conn_info[1])
s3.upload_file(local_file, s3_conn_info[2], 'pipeline/mongo_extract.csv')

# 실행 종료 시각
cost_time_sec = time.time() - start_time
cost_time_min = cost_time_sec / 60
cost_time_hour = cost_time_min / 60
print(f">> 소요 시간 {cost_time_sec}초 = {cost_time_min}분 {cost_time_hour:}시간")
