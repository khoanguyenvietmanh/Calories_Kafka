from time import sleep

import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import logging 

logging.basicConfig(level=logging.INFO)

def publish_message(producer_instance, topic_name, key, value):
    try:
         key_bytes = bytes(key, encoding='utf-8')
         value_bytes = bytes(value, encoding='utf-8')
         producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
         producer_instance.flush()
    except Exception as ex:
        logging.info('Exception while publishing message')
        logging.info(ex)
        


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        logging.info('Exception while connecting Kafka')
        logging.info(ex)
    finally:
        return _producer

def fetch_raw(recipe_url, headers):
    html = None
    logging.info('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url, headers=headers)
        if r.status_code == 200:
            html = r.text
    except Exception as ex:
        logging.info('Exception while accessing raw html')
        logging.info(ex)
    finally:
        return html.strip()
    
def get_recipes(headers):
    recipes = []
    links = ["https://www.allrecipes.com/recipe/20762/california-coleslaw/", "https://www.allrecipes.com/recipe/8584/holiday-chicken-salad/", "https://www.allrecipes.com/recipe/80867/cran-broccoli-salad/"]
    idx = 0
    logging.info('Accessing list')
    try:
        for link in links:
            sleep(2)
            recipe = fetch_raw(link, headers)
            recipes.append(recipe)
            idx += 1        
    except Exception as ex:
        logging.info('Exception in get_recipes')
        logging.info(ex)
    finally:
        return recipes
    

if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    
    all_recipes = get_recipes(headers)
    
    if len(all_recipes) > 0:
        kafka_producer = connect_kafka_producer()
        for recipe in all_recipes:
            publish_message(kafka_producer, 'raw_recipes', 'raw', recipe.strip())
        
        if kafka_producer is not None:
            kafka_producer.close()
        

         
    
         