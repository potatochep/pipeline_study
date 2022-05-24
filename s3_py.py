import boto3
import configparser
import os

s3_file = '/Users/kimhyojin/Desktop/python_daily/python_data/asset/credit.csv'

# s3 접속 정보 불러오기
parser = configparser.ConfigParser()
parser.read(os.getcwd() + os.sep + "/config/pipeline.conf")
access_key = parser.get('aws_boto_credentials','access_key')
secret_key = parser.get('aws_boto_credentials','secret_key')
bucket_name = parser.get('aws_boto_credentials','bucket_name')

s3 = boto3.client(
    's3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key
)

s3.upload_file(s3_file,bucket_name,'pipeline_study/test.csv')
