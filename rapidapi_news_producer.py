from kafka import KafkaProducer
import time

#import logging
#logging.basicConfig(level=logging.DEBUG)

import requests
import json
import random

url = "https://free-news.p.rapidapi.com/v1/search"


def generateNews():
    headers = {
        'x-rapidapi-host': "free-news.p.rapidapi.com",
        'x-rapidapi-key': "5617b3ff62msh12d7e22dd3bc39ap18c3d0jsn9ce8d55207ea"
        }
    key_words = []
    querystring = {"q":"IPL","lang":"en"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    global artcles_list
    res = response.json()
    
    #save articles in list
    artcles_list = res["articles"]



def produce_msgs(artcles_list,hostname='localhost', port='9092', 
                 topic_name='newsarticles',         
                 nr_messages=2,                     
                 max_waiting_time_in_sec=60):
    
    # Function for Kafka Producer with certain settings related to the Kafka's Server
    producer = KafkaProducer(
        bootstrap_servers=hostname+":"+port,api_version=(0,10),
        value_serializer=lambda v: json.dumps(v).encode('ascii'),
        key_serializer=lambda v: json.dumps(v).encode('ascii')
    )
    
    #print(artcles_list)
    j=0
    j=len(artcles_list)
    for i in artcles_list:
        print("Sending: {}".format(i))
     
        producer.send(topic_name,i)
        # Sleeping time
#        sleep_time = random.randint(0, max_waiting_time_in_sec * 10)/10
#        print("Sleeping for..."+str(sleep_time)+'s')
        time.sleep(1)

         # Force flushing of all messages
        if (j % 100) == 0:
             producer.flush()
        j = j + 1
     
    producer.close()
    
generateNews()        
produce_msgs(artcles_list)