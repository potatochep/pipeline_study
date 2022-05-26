import time
import pymysql
import csv
# 접속을 위한 함수 import
from config.func import *
#ssh 접속 정보 가져오기
ssh_conn_info = get_ssh_connect_information()
# 시작 시간
start_time = time.time()

#ssh 접속을 통해 aws ec2 접속
server = ssh_conn(ssh_conn_info[0],'ec2-user',ssh_conn_info[1],'127.0.0.1',3306)
if server is None:
    print("Error connecting to the AWS Server")
else:
    print("AWS Server connecting established")
server.start()
#mysql 접속 정보 가져오기 / mysql 접속
mysql_conn_info = get_mysql_connect_information()
conn = mysql_conn(mysql_conn_info[0],mysql_conn_info[2],mysql_conn_info[3],mysql_conn_info[1],'test_db')
# 접속에 성공 또는 실패확인을 위한 출력
if conn is None:
    print("Error connecting to the MySql database")
else:
    print("MySql connection established")
cursor = conn.cursor()

# mysql 서버에 쿼리 전달 및 쿼리 결과 변수 저장
sql = 'select * from Orders;'
cursor.execute(sql)
results = cursor.fetchall()

# 쿼리의 결과를 csv 파일로 저장
local_filename = 'data/order_extract.csv'
with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)

# 추출 작업 종료 후 접속 끊기
fp.close()
cursor.close()
conn.close()
server.close()

# csv 파일 저장을 위한 aws s3 접속 정보 가져오기 및 s3 접속
s3_conn_info = get_s3_connect_information()
s3 = s3_conn('s3', s3_conn_info[0], s3_conn_info[1])
s3.upload_file(local_filename, s3_conn_info[2], 'pipeline/order_extract.csv')

# 총 작업 소요시간 출력
work_time(start_time)