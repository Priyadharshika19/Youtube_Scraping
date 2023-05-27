
#importing libraries
from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import pymongo
from requests.adapters import ResponseError

import psycopg2
db2=psycopg2.connect(host='localhost', user='postgres', password=318327, port=5432, database="yt")
cursor=db2.cursor()
cursor.execute("DROP table IF EXISTS channel_info;")
db2.commit()
cursor.execute("CREATE TABLE channel_info (id varchar(100), name varchar(200), description text, custom_url text, published_at date, video_count int,view_count int,subscriber_count int, playlist_id varchar(100),PRIMARY KEY(id))")
db2.commit()
cursor.execute("DROP table IF EXISTS video_info;")
db2.commit()
cursor.execute("CREATE TABLE video_info (video_id varchar(100),chan_id varchar(200),video_name varchar(200), video_description text, published_date date, view_count int, fav_count int, thumbnail varchar(200), caption_status varchar(100), duration varchar(200), like_count int, dislike_count int, comment_count int, tags varchar(100), PRIMARY KEY(video_id), FOREIGN KEY(chan_id) REFERENCES channel_info (ID))")
db2.commit()
cursor.execute("DROP table IF EXISTS comment_info;")
db2.commit()
cursor.execute("CREATE TABLE comment_info (comment_id varchar(100), video_id varchar(200), comment_txt text, comment_author text, comment_publish_date date, PRIMARY KEY(comment_id),FOREIGN KEY(video_id) REFERENCES video_info (video_id))")
db2.commit()


def transfer():
    #connection
    client=pymongo.MongoClient("mongodb+srv://priya:generate@cluster0.m4kzbjb.mongodb.net/?retryWrites=true&w=majority")
    db=client["Youtube_Scraping"]
    col=db["Youtube_data"]
    #col.delete_many({})
    channel_dict = {'id': [],
                    'name': [],
                    'description': [],
                    'custom_url': [],
                    'published_at': [],
                    'video_count': [],
                    'view_count': [],
                    'subscriber_count': [],
                    'playlist_id': []}
    vid_dict = {'video_id': [],
                'chan_id': [],
                'video_name': [],
                'video_description': [],
                'published_date': [],
                'view_count': [],
                'fav_count': [],
                'thumbnail': [],
                'caption_status': [],
                'duration': [],
                'like_count': [],
                'dislike_count': [],
                'comment_count': [],
                'tags': []}

    comt_dict = {'comment_id': [],
                 'video_id': [],
                 'comment_txt': [],
                 'comment_author': [],
                 'comment_publish_date': []}

    for rec in col.find({}):
        #print(rec)
        x = rec.get("channel_detail")["channel_info"]
        channel_dict["id"].append(x["channel_id"])
        channel_dict["name"].append(x["channel_name"])
        channel_dict["description"].append(x["channel_description"])
        channel_dict["custom_url"].append(x["channel_custom_url"])
        channel_dict["published_at"].append(x["channel_published_at"])
        channel_dict["video_count"].append(x["channel_videocount"])
        channel_dict["view_count"].append(x["channel_viewcount"])
        channel_dict["subscriber_count"].append(x["channel_subscribercount"])
        channel_dict["playlist_id"].append(x["channel_playlist_id"])
        print(channel_dict)
        for v in range(0, len(rec.get("channel_detail")["video_info"])):
            y = rec.get("channel_detail")["video_info"][v]
            # print(len(rec.get("channel_detail")["video_info"]))
            vid_dict["video_id"].append(y["video_id"])
            vid_dict["chan_id"].append(y["chan_id"])
            vid_dict["video_name"].append(y["video_name"])
            vid_dict["video_description"].append(y["video_description"])
            vid_dict["published_date"].append(y["published_date"])
            vid_dict["view_count"].append(y["view_count"])
            vid_dict["fav_count"].append(y["fav_count"])
            vid_dict["thumbnail"].append(y["thumbnail"])
            vid_dict["caption_status"].append(y["caption_status"])
            #print(y)
            try:
                vid_dict["duration"].append(y["duration"])
            except:
                vid_dict["duration"].append(0)
            vid_dict["like_count"].append(y["like_count"])
            vid_dict["dislike_count"].append(y["dislike_count"])
            vid_dict["comment_count"].append(y["comment_count"])
            vid_dict["tags"].append(y["tags"])
            print(vid_dict)
        for c in range(0, len(rec.get("channel_detail")["comment_info"])):
            z = rec.get("channel_detail")["comment_info"][c]
            # print(type(z))
            comt_dict["comment_id"].append(z["comment_id"])
            comt_dict["video_id"].append(z["video_id"])
            comt_dict["comment_txt"].append(z["comment_txt"])
            comt_dict["comment_author"].append(z["comment_author"])
            comt_dict["comment_publish_date"].append(z["comment_publish_date"])
            print(comt_dict)
    df1 = pd.DataFrame(channel_dict)
    #print(df1)
    for i, row in df1.iterrows():
        sql = "INSERT INTO channel_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.execute(sql, tuple(row))
        except:
            pass
        db2.commit()
    df2 = pd.DataFrame(vid_dict)
    for i, row in df2.iterrows():
        sql = "INSERT INTO video_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cursor.execute(sql, tuple(row))
        except:
            pass
        db2.commit()
    df3 = pd.DataFrame(comt_dict)
    for i, row in df3.iterrows():
        sql = "INSERT INTO comment_info VALUES (%s,%s,%s,%s,%s)"
        try:
            cursor.execute(sql, tuple(row))
        except:
            pass
        db2.commit()

transfer()