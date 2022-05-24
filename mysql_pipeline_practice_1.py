import time
from sqlalchemy import create_engine
import pymysql
from sshtunnel import SSHTunnelForwarder
import configparser
import os
import pandas as pd

# 파일 위치 설정
print('work dir = ', os.getcwd())
print('seperator = ', os.sep)

#ssh 접속 및 mysql 접속 정보 읽어오기
parser = configparser.ConfigParser()
parser.read(os.getcwd() + os.sep + "/config/pipeline.conf") 

#ssh 접속 정보
server_ip = parser.get('ssh_key_path', 'server_ip')
key = parser.get('ssh_key_path', 'key')

# 시작 시간
start_time = time.time()

#ssh 접속을 통해 aws ec2 접속
server = SSHTunnelForwarder(
    (server_ip, 22),
    ssh_username='ec2-user',
    ssh_pkey=key,
    remote_bind_address=('127.0.0.1', 3306),
    local_bind_address=('127.0.0.1',3306)
)
server.start()

#mysql 접속 정보
hostname = parser.get('mysql_config', 'hostname')
port = parser.get('mysql_config', 'port')
username = parser.get('mysql_config', 'username')
password = parser.get('mysql_config', 'password')

# ssh 접속 후 docker 환경으로 구성된 mysql 서버 접속 - engine 생성
engine = create_engine(f"mysql+pymysql://{username}:{password}@{hostname}:3306/test_db", pool_pre_ping=True)
# pandas 활용 csv 파일 mysql 전송
df = pd.read_csv("/Users/kimhyojin/Desktop/python_daily/python_data/asset/credit.csv", encoding='utf-8')

table_name = 'etl_test_2'
# DB에 DataFram 적재
df.to_sql(
    index= False,
    name = table_name,
    con = engine,
    if_exists = 'replace',
    method = 'multi'
)

# 실행 종료 시각
cost_time_sec = time.time() - start_time
cost_time_min = cost_time_sec / 60
cost_time_hour = cost_time_min / 60

# 실행 시각 출력
print(f">> 소요 시간 {cost_time_sec}초 = {cost_time_min}분 {cost_time_hour:}시간")

server.close()