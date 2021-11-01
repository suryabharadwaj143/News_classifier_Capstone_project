import pyspark
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import col
import pandas as pd
#import matplotlib.pyplot as plt
import pickle
#import seaborn as sns
import nltk
from nltk.corpus import stopwords
# from wordcloud import WordCloud, STOPWORDS
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from nltk.tokenize import word_tokenize
from sklearn.pipeline import Pipeline
from sklearn.pipeline import FeatureUnion
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
import pyspark
import json
import string
import pandas as pd
import swifter
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import numpy as np
from sklearn.preprocessing import LabelEncoder

import re
import warnings

warnings.filterwarnings("ignore")

directory="./jars/*"
saveto_path="./News_Clf_Model.pkl"

spark = SparkSession \
    .builder \
    .appName("NewsClassifier_datafeed") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/news_database.news_feed") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/news_database.news_feed") \
    .config('spark.driver.extraClassPath', directory) \
    .getOrCreate()


print(spark)

df = spark.read.format("mongo").load()

print("Schema:")
df.printSchema()

columns_to_drop = ['author', 'authors', 'is_opinion', 'media','_id','_score','clean_url','country','language','link','published_date','published_date_precision','rank','rights','twitter_account']
df = df.drop(*columns_to_drop)
#print("after cleaning and dropping the columns\n")
#df.show(5)
#print("")
#df.columns

def stopWords_remover(text):
    text=text.replace('\n', ' ').replace('\r', '').strip()
    text=re.sub(' +', ' ', text)
    text=re.sub(r'[^\w\s]', '', text)
    text = text.lower()
    stop_words=set(stopwords.words('english'))
    word_tokens=word_tokenize(text)
    filtered_sentence=[w for w in word_tokens if not w in stop_words]
    filtered_sentence=[]
    for w in word_tokens:
        if w not in stop_words:
            filtered_sentence.append(w)

    text=" ".join(filtered_sentence)
    return text

# removing the values which has both null in title , summary
df=df.filter(df.title.isNotNull() & col("summary").isNotNull())
pd_Df=df.toPandas()
pd_Df["title"].fillna('summary', inplace=True)
pd_Df["summary"].fillna('title', inplace=True)
pd_Df["topic"].fillna("General news", inplace=True)
#sending the input to model as title + summary
pd_Df['title_plusSummary_txt']=pd_Df['title'] + pd_Df['summary']

pd_Df['model_input_txt'] = pd_Df['title_plusSummary_txt'].apply(stopWords_remover)
# Label Encoding

X=pd_Df[['model_input_txt']]
y=pd_Df['topic']

encoder=LabelEncoder()
y=encoder.fit_transform(y)

x_train, x_test, y_train, y_test=train_test_split(X, y, test_size=0.2)

dict_encoded_pred=dict(zip(list((y)), pd_Df['topic'].to_list()))
with open('Topics_pred.csv', 'w') as f:
    for key in dict_encoded_pred.keys():
        f.write("%d,%s\n"%(key,dict_encoded_pred[key]))
#print(dict_encoded_pred)


# Training the Model
news_clf=Pipeline(
    [('vect', CountVectorizer(analyzer="word", stop_words="english")), ('tfidf', TfidfTransformer(use_idf=True)),
    ('clf', MultinomialNB(alpha=.01))])

news_clf.fit(x_train['model_input_txt'].to_list(), list(y_train))
# Testing Model
X_TEST=x_test['model_input_txt'].to_list()
Y_TEST=list(y_test)

predicted=news_clf.predict(X_TEST)

np.mean(predicted == Y_TEST)

# Prediction
test_text=['The World Health Organization (WHO) announced 26 proposed members to an advisory committee aimed to steer studies into the origin of the COVID-19 pandemic and other pathogens of epidemic potential.']
predicted=news_clf.predict(test_text)
print(dict_encoded_pred[predicted[0]])

#saving the model;
#with open(saveto_path,'wb') as f:
 #   pickle.dump(news_clf,f)

