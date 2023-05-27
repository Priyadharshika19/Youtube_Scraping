Step1:   Create Youtube  API key
Go to the browser google developer consolecloud overviewcreate projectgive name ok
Goto api serviceenablescroll downyoutube data api  v3managecreate consolepublic dataapi
My yt api:          ABCe2yklwjxI24zpi80jKtc44VNnLs
 Web page: https://console.cloud.google.com/apis/api/youtube.googleapis.com/metrics?project=yt-analysis-386212&supportedpurview=project

Step2:
Youtube data api     in searchbar   click first linkweb site:      https://developers.google.com/youtube/v3

Step3:
Pip install google-api-python-client
->Import modules:  
import streamlit as st
import psycopg2
from googleapiclient.discovery import build
import pandas as pd
import pymongo

->Create connections:
Give your username and password
1.Connection  to sql database
import psycopg2
db2=psycopg2.connect(host='localhost', user='********', password=*******, port=5432, database="yt")
cursor=db2.cursor()

2. Connection  to No- sql database
client=pymongo.MongoClient("mongodb+srv://username:******@cluster0.m4kzbjb.mongodb.net/?retryWrites=true&w=majority")

Step 4:
Extract youtube informations
--Channel  details
--Videos  details
--Comments  details
Step 5: 
Store processed informations  in MongoDB
Step 6:
Get the informations from MongoDB and store it in SQL Database(postgres/mysql/..)
Step 7:
Create queries and explore the details with the help of Streamlit









	



