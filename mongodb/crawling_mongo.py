from bs4 import BeautifulSoup
import requests
import re
from pymongo import mongo_client
import sys
import os
import time
from mongo_func import *
from datetime import datetime
import csv
# 시작 시간
start_time = time.time()
#ssh 접속 정보 가져오기 및 aws ec2 접속
ssh_conn_info = get_ssh_connect_information()
server = ssh_conn(ssh_conn_info[0],'ec2-user',ssh_conn_info[1],'127.0.0.1',27017)\

# 접속 상태 확인
if server is None:
    print("#### Error connecting to the AWS Server ####")
else:
    print("#### AWS Server connecting established ####")
    
server.start()

# 몽고db 접속 정보 가져오기 및 연결
mongo_conn_info = get_mongo_connect_information()
conn = mongo_client.MongoClient(f"mongodb://{mongo_conn_info[2]}:{mongo_conn_info[3]}@{mongo_conn_info[0]}:{mongo_conn_info[1]}")

# 접속 상태 확인
if conn is None:
    print("#### Error connecting to the MONGO ####")
else:
    print("#### MONGO connecting established ####")


actor_db = conn.cine21
actor_collection = actor_db.actor_collection


actors_detail = []

cine21_url = 'http://www.cine21.com/rank/person/content'
post_data = dict()
post_data['section'] = 'actor'
post_data['period_start'] = '2021-05'
post_data['gender'] = 'all'

for page_num in range(1,21):
    page_start_time = time.time()
    post_data['page'] = page_num
    
    res = requests.post(cine21_url,data=post_data)
    soup = BeautifulSoup(res.content,"html.parser")
    
    actors = soup.select('li.people_li div.name')
    hits = soup.select('ul.num_info > li > strong')
    movies = soup.select(('ul.mov_list'))
    rankings = soup.select('li.people_li > span.grade')

    for index , j in enumerate(actors):
        
        actor_name = re.sub('\(\w*\)','',j.text)
        actor_hits = int(hits[index].text.replace(',',''))
        movie_titles = movies[index].select('li a span')
        movie_title_list = []
        for movie in movie_titles:
            movie_title_list.append(movie.text)   
        movie_title_list

        actor_link = 'http://www.cine21.com' + j.select_one("a").attrs['href']
        response_actor = requests.get(actor_link)
        soup_actor = BeautifulSoup(response_actor.content, 'html.parser')
        default_info = soup_actor.select_one('ul.default_info')
        actor_detail_info = default_info.select('li')

        actor_info_dict = dict()

        actor_info_dict['배우이름'] = actor_name
        actor_info_dict['흥행지수'] = actor_hits
        actor_info_dict['출연영화'] = movie_title_list
        actor_info_dict['랭킹'] = rankings[index].text

        for k in actor_detail_info:
            actor_item_field = k.select_one('span.tit').text
            actor_itme_value = re.sub('<span.*?>.*?</span>','',str(k))
            actor_itme_value = re.sub('<.*?>','',actor_itme_value)
            actor_info_dict[actor_item_field] = actor_itme_value
        
        actors_detail.append(actor_info_dict)

    print(f"{page_num} 페이지 작업 완료")
    work_time(page_start_time)


actor_collection.insert_many(actors_detail)

server.close()
conn.close()

work_time(start_time)