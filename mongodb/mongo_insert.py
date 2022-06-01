from dataclasses import dataclass
from pymongo import mongo_client
import sys
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from config.func import *
from datetime import datetime

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

event_1 = {
    "event_id":1,
    "event_timestamp": datetime.today(),
    "event_name":"signup"
}

event_2 = {
    "event_id":2,
    "event_timestamp": datetime.today(),
    "event_name":"pageview"
}

event_3 = {
    "event_id":3,
    "event_timestamp": datetime.today(),
    "event_name":"login"
}

mongo_collection.insert_one(event_1)
mongo_collection.insert_one(event_2)
mongo_collection.insert_one(event_3)

# 실행 종료 시각
cost_time_sec = time.time() - start_time
cost_time_min = cost_time_sec / 60
cost_time_hour = cost_time_min / 60
print(f">> 소요 시간 {cost_time_sec}초 = {cost_time_min}분 {cost_time_hour:}시간")

# 작업 종료후 연결 종료
server.close()
conn.close()