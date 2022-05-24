from sshtunnel import SSHTunnelForwarder
import configparser
from pymongo import mongo_client
import os
import time

# 파일 위치 설정
print('work dir = ', os.getcwd())
print('seperator = ', os.sep)

#ssh 접속 및 mysql 접속 정보 읽어오기
parser = configparser.ConfigParser()
parser.read('/Users/kimhyojin/Desktop/pipeline/config/pipeline.conf') 

#ssh 접속 정보
server_ip = parser.get('ssh_key_path', 'server_ip')
key = parser.get('ssh_key_path', 'key')

# 몽고 db 접속 정보
hostname = parser.get('mongo_config', 'hostname')
port = parser.get('mongo_config', 'port')
username = parser.get('mongo_config', 'username')
password = parser.get('mongo_config', 'password')

# 시작 시간
start_time = time.time()

#ssh 접속을 통해 aws ec2 접속
server = SSHTunnelForwarder(
    (server_ip, 22),
    ssh_username='ec2-user',
    ssh_pkey=key,
    remote_bind_address=('127.0.0.1', 27017),
    local_bind_address=('127.0.0.1',27017)
)
server.start()

# 몽고db 연결하기
conn = mongo_client.MongoClient(f"mongodb://{username}:{password}@{hostname}:{port}")

# 데이터베이스 연결 후 작업 실행
db = conn.knowledge
print(db)

it = db.it
it

# 실행 종료 시각
cost_time_sec = time.time() - start_time
cost_time_min = cost_time_sec / 60
cost_time_hour = cost_time_min / 60
print(f">> 소요 시간 {cost_time_sec}초 = {cost_time_min}분 {cost_time_hour:}시간")

# 작업 종료후 연결 종료
server.close()
conn.close()