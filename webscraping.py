import requests
import nltk
from bs4 import BeautifulSoup
import numpy as np 
import pandas as pd 
import seaborn as sns
import matplotlib.pyplot as plt
%matplotlib inline
import random
from wordcloud import WordCloud
import os
import spacy
nlp = spacy.load('en_core_web_sm')
from textblob import TextBlob
from pattern.en import sentiment

for i in range(1, 3661):
   url = "https://au.trustpilot.com/review/afterpay.com?page=
   {}".format(i)
   result = requests.get(url)
   soup = BeautifulSoup(result.content)
   reviews = soup.find_all('article', class_='review')
   for review in reviews:
 
      allUsers.append(review('aside')[0]('a')[0]('div', 
      class_='consumer-information__details')[0]('div',  
      class_='consumer-information__name')[0].string)

      Dictionary using json module
      ratingsDic = json.loads(review('script')[0].string)
      allRatings.append(ratingsDic['stars'])
      if(not(review('aside')[0]('a')[0]('div', class_='consumer-  
      information__details')[0]('div', class_='consumer- 
      information__data')[0]('div', class_='consumer-
      information__location')) == []):
         allLocations.append(review('aside')[0]('a')[0]('div', 
         class_='consumer-information__details')[0]('div', 
         class_='consumer-information__data')[0]('div',    
         class_='consumer-information__location')[0]('span')
         [0].string)
      else:
         allLocations.append("")
      datesDic = json.loads(review('div', class_='review-content-
      header__dates')[0]('script')[0].string)
      allDates.append(datesDic['publishedDate'])
      reviewDic = json.loads(review('script')[0].string)
      allReviewContent.append(reviewDic['reviewHeader'])

      review_data = pd.DataFrame(
{
   'name':allUsers,
   'rating':allRatings,
   'location':allLocations,
   'date':allDates,
   'content':allReviewContent
})
review_data = review_data.replace('\n','',regex=True)

review_data['date']=review_data['date'].str.replace('T', ' ')
review_data['date'] = review_data['date'].map(lambda x: str(x)[:-14])
review_data['date'] = pd.to_datetime(review_data['date'])

def generator(data):
   for c, line in enumerate(data):
   yield{
      '_index': 'trustpilot',
      '_type': '_doc',
      '_id': line.get('id', None),
      '_source':
      {
         'name': line.get('name', ''),
         'rating': line.get('rating', ''),
         'location': line.get('location', ''),
         'date': line.get('date', ''),
         'content': line.get('content', None)
      }
   }
raise StopIteration

ENDPOINT = 'http://localhost:9200/'
es = Elasticsearch(timeout=600, hosts=ENDPOINT)
es.ping()
trustpilot_index = es.indices.create(index='trustpilot', ignore=[400,404])
review_dict = review_data.to_dict('records')
try:
   res = helpers.bulk(es, generator(df2))
except Exception as e:
   pass