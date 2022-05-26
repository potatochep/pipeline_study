from dataclasses import dataclass
from pymongo import mongo_client
import sys
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from config.func import *
from datetime import datetime
import json
from bson import json_util
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
db = conn["cine21"]
print(db)
mongo_collection = db['actor_collection']
print(mongo_collection)

docs = mongo_collection.find()
data = dict()
for doc in docs:
    doc
# local_file = '/Users/kimhyojin/Desktop/pipeline/data/actor_ranking_1yr.json'
# with open(local_file, 'w') as fp:
#     csvw = csv.writer(fp, delimiter='|')
#     csvw.writerows(data)


# fp.close()
server.close()
conn.close()

# csv 파일 저장을 위한 aws s3 접속 정보 가져오기 및 s3 접속
# s3_conn_info = get_s3_connect_information()
# s3 = s3_conn('s3', s3_conn_info[0], s3_conn_info[1])
# s3.upload_file(local_file, s3_conn_info[2], 'pipeline/actor_ranking_1yr.csv')

# 실행 종료 시각 출력
work_time(start_time)
