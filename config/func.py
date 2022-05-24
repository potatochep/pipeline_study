def ssh_conn(server_ip, user_name, key, bind_ip,bind_port):
    from sshtunnel import SSHTunnelForwarder
    server = SSHTunnelForwarder(
        (server_ip, 22),
        ssh_username=user_name,
        ssh_pkey=key,
        remote_bind_address=(bind_ip, bind_port),
        local_bind_address=(bind_ip,bind_port)
    )
    return server

def mysql_conn(hostname,username,password,port,database):
    import pymysql
    conn = pymysql.connect(
        host= hostname,
        user = username,
        password = password,
        port= int(port),
        db= database
    )
    return conn


def work_time(start_time):
    import time
    cost_time_sec = time.time() - start_time
    cost_time_min = cost_time_sec / 60
    cost_time_hour = cost_time_min / 60
    print(f">> 소요 시간 {cost_time_sec}초 = {cost_time_min}분 {cost_time_hour:}시간")

def get_ssh_connect_information():
    from configparser import ConfigParser
    #ssh 접속 및 mysql 접속 정보 읽어오기
    parser = ConfigParser()
    parser.read('/Users/kimhyojin/Desktop/pipeline/config/pipeline.conf') 
    #ssh 접속 정보
    server_ip = parser.get('ssh_key_path', 'server_ip')
    key = parser.get('ssh_key_path', 'key')

    return (server_ip,key)


def get_mysql_connect_information():
    from configparser import ConfigParser
    # mysql 접속 정보 가져오기
    parser = ConfigParser()
    parser.read('/Users/kimhyojin/Desktop/pipeline/config/pipeline.conf') 
    #mysql 접속 정보
    hostname = parser.get('mysql_config', 'hostname')
    port = parser.get('mysql_config', 'port')
    username = parser.get('mysql_config', 'username')
    password = parser.get('mysql_config', 'password')

    return (hostname,port,username,password)

def get_s3_connect_information():
    from configparser import ConfigParser
    parser = ConfigParser()
    parser.read('/Users/kimhyojin/Desktop/pipeline/config/pipeline.conf') 
    # s3 접속 정보 가져오기
    access_key = parser.get('aws_boto_credentials','access_key')
    secret_key = parser.get('aws_boto_credentials','secret_key')
    bucket_name = parser.get('aws_boto_credentials','bucket_name')
    return (access_key, secret_key, bucket_name)

def s3_conn(name,access_key,secret_key):
    import boto3
    s3 = boto3.client(
        name,
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key
    )
    return s3

def get_mongo_connect_information():
    from configparser import ConfigParser
    # mysql 접속 정보 가져오기
    parser = ConfigParser()
    parser.read('/Users/kimhyojin/Desktop/pipeline/config/pipeline.conf') 
    #mysql 접속 정보
    hostname = parser.get('mongo_config', 'hostname')
    port = parser.get('mongo_config', 'port')
    username = parser.get('mongo_config', 'username')
    password = parser.get('mongo_config', 'password')

    return (hostname,int(port),username,password)
