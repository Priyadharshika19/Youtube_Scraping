from googleapiclient.discovery import build
import pandas as pd
import numpy as np
import streamlit as st
from streamlit_option_menu import option_menu
import pymongo
import re
import psycopg2
db2=psycopg2.connect(host='localhost', user='postgres', password=******, port=5432, database="yt")
cursor=db2.cursor()

#cursor.execute("DROP table IF EXISTS comment_info;")
#db2.commit()
#cursor.execute("DROP table IF EXISTS video_info;")
#db2.commit()
#cursor.execute("DROP table IF EXISTS channel_info;")
#db2.commit()
#cursor.execute("CREATE TABLE channel_info (id varchar(100), name varchar(200), description text, custom_url text, published_at date, video_count int,view_count int,subscriber_count int, playlist_id varchar(100),PRIMARY KEY(id))")
#db2.commit()
#cursor.execute("CREATE TABLE video_info (video_id varchar(100),chan_id varchar(200),video_name varchar(200), video_description text, published_date date, view_count int, fav_count int, thumbnail varchar(200), caption_status varchar(100), duration int, like_count int, dislike_count int, comment_count int, tags varchar(100), PRIMARY KEY(video_id), FOREIGN KEY(chan_id) REFERENCES channel_info (ID))")
#db2.commit()
#cursor.execute("CREATE TABLE comment_info (comment_id varchar(100), video_id varchar(200), comment_txt text, comment_author text, comment_publish_date date, PRIMARY KEY(comment_id),FOREIGN KEY(video_id) REFERENCES video_info (video_id))")
#db2.commit()

#client=pymongo.MongoClient("mongodb+srv://priya:generate@cluster0.m4kzbjb.mongodb.net/?retryWrites=true&w=majority")
client=pymongo.MongoClient("mongodb://priya:generate@ac-ljtqqfr-shard-00-00.m4kzbjb.mongodb.net:27017,ac-ljtqqfr-shard-00-01.m4kzbjb.mongodb.net:27017,ac-ljtqqfr-shard-00-02.m4kzbjb.mongodb.net:27017/?ssl=true&replicaSet=atlas-b5xh3w-shard-0&authSource=admin&retryWrites=true&w=majority")
db=client["Youtube_Scraping"]
col=db["Youtube_data"]
#col.delete_many({})
st.set_page_config(page_title="Youtube Scraping",page_icon=":tada",layout="wide")
st.title("YOUTUBE SCRAPING")

page_bg_img= """
<style>
[data-testid="stAppViewContainer"]
{
background-color: #cfd3ce;
}  
[data-testid="stHeader"]
{
background-color: #5a83a8;
text-align: center;
}
[data-testid="stSidebar"]
{
text-decoration-color: #5a83a8;
background-color: #5a83a8;

}
</style>
"""
st.markdown(page_bg_img, unsafe_allow_html=True)

def channel(youtube, channel_id):
    c_id_request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=channel_id)
    c_id_response = c_id_request.execute()
    info=c_id_response.get("items")[0]

    data={
        "channel_id":info["id"],
        "channel_name":info["snippet"]["title"],
        "channel_description":info["snippet"]["description"],
        "channel_custom_url":info["snippet"]["customUrl"],
        "channel_published_at":info["snippet"]["publishedAt"][:10],
        "channel_videocount":info["statistics"]["videoCount"],
        "channel_viewcount":info["statistics"]["viewCount"],
        "channel_subscribercount":info["statistics"]["subscriberCount"],
        "channel_playlist_id":info["contentDetails"]["relatedPlaylists"]["uploads"],
         }
    pl_id=info["contentDetails"]["relatedPlaylists"]["uploads"]
    return data ,pl_id

def video_id(youtube,pl_id):
   v_request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=pl_id,
                maxResults = 50
            )
   v_response = v_request.execute()
   video_ids=[]
   for i in range(len(v_response['items'])):
       video_ids.append(v_response['items'][i]['contentDetails']['videoId'])
   next_page_token = v_response.get('nextPageToken')
   more_pages = True

   while more_pages:
       if next_page_token is None:
           more_pages = False
       else:
           request = youtube.playlistItems().list(
                       part='contentDetails',
                       playlistId = pl_id,
                       maxResults = 50,
                       pageToken = next_page_token)
           v_response = request.execute()
           for i in range(len(v_response['items'])):
               video_ids.append(v_response['items'][i]['contentDetails']['videoId'])
           next_page_token = v_response.get('nextPageToken')
   return video_ids


def video_d(youtube, video_ids):
    all_video_details = []
    for vid in range(0, len(video_ids), 50):
        v_info_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids[vid:vid + 50])
        )
        v_info_response = v_info_request.execute()

        for video in v_info_response['items']:
            vid_data = {
                "video_id": video["id"],
                "chan_id": video['snippet']["channelId"],
                "video_name": video["snippet"]["title"],
                "video_description": video["snippet"]["description"],
                "published_date": video["snippet"]["publishedAt"][:10],
                "view_count": video["statistics"]["viewCount"],
                "fav_count": video["statistics"]["favoriteCount"],
                "thumbnail": video['snippet']["thumbnails"]["default"]["url"],
                "caption_status": video['contentDetails']["caption"]
            }
            try:
                e = video['contentDetails']["duration"][2:]
                hr = re.findall(r'(\d+)H', e)
                try:
                    hr = int[hr[0]]
                except:
                    hr = 0
                min = re.findall(r'(\d+)M', e)
                min = int(min[0])
                sec = re.findall(r'(\d+)S', e)
                sec = int(sec[0])
                sum = (hr * 60) + min + (sec / 60)
                in_min = int(sum)
                vid_data["duration"] = in_min
            except:
                vid_data["duration"] = 1
            try:
                vid_data["like_count"] = video["statistics"]["likeCount"]
            except:
                vid_data["like_count"] = None

            try:
                vid_data["dislike_count"] = video["statistics"]["dislikeCount"]
            except:
                vid_data["dislike_count"] = None

            try:
                vid_data["comment_count"] = video["statistics"]["commentCount"]
            except:
                vid_data["comment_count"] = None

            try:
                vid_data["tags"] = video['snippet']["tags"]
            except:
                vid_data["tags"] = None

            all_video_details.append(vid_data)


    return all_video_details


def comment_d(youtube, video_ids):
    comment_info = []
    for x in video_ids:
        cmd_data = {}
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=x,
            maxResults=50

        )
        try:
            response = request.execute()
        except:
            pass
        try:
            c_response = response['items'][0]
            cmd_data["comment_id"] = c_response["id"]
            cmd_data["video_id"] = c_response["snippet"]["videoId"]
            lst = []
            for l in range(0, len(response['items'])):
                a = response['items'][l]["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                lst.append(a)
            cmd_data["comment_txt"] = lst
            cmd_data["comment_author"] = c_response["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
            cmd_data["comment_publish_date"] = c_response["snippet"]["topLevelComment"]["snippet"]["publishedAt"][:10]
            comment_info.append(cmd_data)
        except:
            pass
    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=x,
                maxResults=50

            )
            try:
                response = request.execute()
            except:
                pass
            try:
                c_response = response['items'][0]
                cmd_data["comment_id"] = c_response["id"]
                cmd_data["video_id"] = c_response["snippet"]["videoId"]
                lst = []
                for l in range(0, len(response['items'])):
                    a = response['items'][l]["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                    lst.append(a)
                cmd_data["comment_txt"] = lst
                cmd_data["comment_author"] = c_response["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                cmd_data["comment_publish_date"] = c_response["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                comment_info.append(cmd_data)
            except:
                pass
            next_page_token = response.get('nextPageToken')

    return comment_info

def transfer(selected):

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
        if rec.get("channel_detail")["channel_info"]["channel_name"]== selected:
            #print(rec)
            x = rec.get("channel_detail")["channel_info"]
            channel_dict["id"].append(x["channel_id"])
            channel_dict["name"].append(x["channel_name"])
            channel_dict["description"].append(x["channel_description"])
            channel_dict["custom_url"].append(x["channel_custom_url"])
            channel_dict["published_at"].append(x["channel_published_at"][:10])
            channel_dict["video_count"].append(x["channel_videocount"])
            channel_dict["view_count"].append(x["channel_viewcount"])
            channel_dict["subscriber_count"].append(x["channel_subscribercount"])
            channel_dict["playlist_id"].append(x["channel_playlist_id"])
            for v in range(0, len(rec.get("channel_detail")["video_info"])):
                y = rec.get("channel_detail")["video_info"][v]
                vid_dict["video_id"].append(y["video_id"])
                vid_dict["chan_id"].append(y["chan_id"])
                vid_dict["video_name"].append(y["video_name"])
                vid_dict["video_description"].append(y["video_description"])
                vid_dict["published_date"].append(y["published_date"][:10])
                vid_dict["view_count"].append(y["view_count"])
                vid_dict["fav_count"].append(y["fav_count"])
                vid_dict["thumbnail"].append(y["thumbnail"])
                vid_dict["caption_status"].append(y["caption_status"])
                try:
                    vid_dict["duration"].append(y["duration"])
                except:
                    vid_dict["duration"].append(0)
                vid_dict["like_count"].append(y["like_count"])
                vid_dict["dislike_count"].append(y["dislike_count"])
                vid_dict["comment_count"].append(y["comment_count"])
                vid_dict["tags"].append(y["tags"])
            for c in range(0, len(rec.get("channel_detail")["comment_info"])):
                z = rec.get("channel_detail")["comment_info"][c]
                comt_dict["comment_id"].append(z["comment_id"])
                comt_dict["video_id"].append(z["video_id"])
                comt_dict["comment_txt"].append(z["comment_txt"])
                comt_dict["comment_author"].append(z["comment_author"])
                comt_dict["comment_publish_date"].append(z["comment_publish_date"][:10])
    df1 = pd.DataFrame(channel_dict)
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

def data_frame(q):
    cursor.execute(q)
    my_tuple_list = cursor.fetchall()
    return my_tuple_list
def channel_detail(channel_id):
    api_key = 'AIzaSyCJUmmre2yklwjxI24zpi80jKtc44VNnLs'
    youtube = build('youtube', 'v3', developerKey=api_key)

    chan_D, pl_id = channel(youtube, channel_id)  # channnel details

    vid_Ids = video_id(youtube, pl_id)  # video details

    vid_D = video_d(youtube, vid_Ids)

    comment_D = comment_d(youtube, vid_Ids)
    data = {}
    data["channel_detail"] = {"channel_info": chan_D, "video_info": vid_D, "comment_info": comment_D}
    col.insert_one(data)
    return chan_D,vid_D,comment_D,data


with st.sidebar:
    selected_page = option_menu(
        "Main Menu",
        ("Extract data","Migrate","Questions","Contact Detail")
    )

if selected_page=="Extract data":
    #channel_id = 'UCmhRdtdgJCa7mg4Jjv4zAmA'  # tridentinfo   1
    channel_id = st.text_input("Enter the channel")
    submit1 = st.button("Submit")
    if submit1:
        try:
            chan_D, vid_D, comment_D, data = channel_detail(channel_id)
            st.write("Data extracted and stored in No-SQL Database successfully")
            st.write(data)

        except:
            st.write('Enter valid channel Id')

if selected_page=="Migrate":
    list_of_channels = []
    for rec in col.find({}):
        ch_lst = rec.get("channel_detail")["channel_info"]["channel_name"]
        list_of_channels.append(ch_lst)
    channel_name=st.selectbox("List of Channels",(list_of_channels))
    if st.button("Migrate Data into SQL Database"):
        transfer(channel_name)
        st.write("Migrated successfully")

    st.markdown(
        '__<p style="font-family:sans-serif; font-size: 20px;">Channel Information</p>__',
        unsafe_allow_html=True)
    cursor.execute(f"Select * From channel_info ")
    my_tuple_list_c = cursor.fetchall()
    df_c=pd.DataFrame(my_tuple_list_c,columns=["id", "name", "description", "custom_url", "published_at", "video_count", "view_count", "subscriber_count", "playlist_id" ])
    df_c.index = [i + 1 for i in range(len(df_c))]
    st.write(df_c)

    st.markdown(
        '__<p style="font-family:sans-serif; font-size: 20px;">Video Information</p>__',
        unsafe_allow_html=True)
    cursor.execute(f"Select * From video_info ")
    my_tuple_list_v = cursor.fetchall()
    df_v = pd.DataFrame(my_tuple_list_v,
                      columns=["video_id", "chan_id", "video_name","video_description", "published_date", "view_count", "fav_count", "thumbnail", "caption_status", "duration", "like_count", "dislike_count", "comment_count", "tags"])
    df_v.index = [i + 1 for i in range(len(df_v))]
    st.write(df_v)

    st.markdown(
        '__<p style="font-family:sans-serif; font-size: 20px;">Comment Information</p>__',
        unsafe_allow_html=True)
    cursor.execute(f"Select * From comment_info ")
    my_tuple_list_cmt = cursor.fetchall()
    df_cmt = pd.DataFrame(my_tuple_list_cmt,
                      columns=["comment_id", "video_id", "comment_txt", "comment_author", "comment_publish_date"])
    df_cmt.index = [i + 1 for i in range(len(df_cmt))]
    st.write(df_cmt)

if selected_page=="Questions":
    selected = st.selectbox("Choose Your Option",
                            (["1. What are the names of all the videos and their corresponding channels?",
                              "2. Which channels have the most number of videos, and how many videos do they have?",
                              "3.	What are the top 10 most viewed videos and their respective channels?",
                              "4.	How many comments were made on each video, and what are their corresponding video names?",
                              "5.	Which videos have the highest number of likes, and what are their corresponding channel names?",
                              "6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                              "7.	What is the total number of views for each channel, and what are their corresponding channel names?",
                              "8.	What are the names of all the channels that have published videos in the year 2022?",
                              "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                              "10. Which videos have the highest number of comments, and what are their corresponding channel names?"]))

    if selected == "1. What are the names of all the videos and their corresponding channels?":
        query = "select channel_info.name, video_info.video_name from video_info join channel_info on video_info.chan_id = channel_info.id order by channel_info.name"
        df = data_frame(query)
        # df.index = [i + 1 for i in range(len(df))]
        df1 = pd.DataFrame(df, columns=["channel_name", "video_name"])
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)
    elif selected == "2. Which channels have the most number of videos, and how many videos do they have?":
        query = "select name, video_count as No_of_Videos from channel_info order by video_count desc limit 10"
        df = data_frame(query)
        df1 = pd.DataFrame(df, columns=["channel_name", "videos_count"])
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)
    elif selected == "3.	What are the top 10 most viewed videos and their respective channels?":
        query = "select channel_info.name, video_info.video_name, video_info.view_count from video_info join channel_info on video_info.chan_id = channel_info.id order by video_info.view_count desc limit 10;"
        df = data_frame(query)
        df1 = pd.DataFrame(df, columns=["channel_name", "video_name", "view_count"])
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)
    elif selected == "4.	How many comments were made on each video, and what are their corresponding video names?":
        query = "select video_name, comment_count from video_info order by comment_count desc"
        df = data_frame(query)
        df1 = pd.DataFrame(df, columns=["video_name", "comment_count"])
        df1["comment_count"] = df1["comment_count"].replace(0, np.nan)
        df1.dropna(inplace=True)
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)
    elif selected == "5.	Which videos have the highest number of likes, and what are their corresponding channel names?":
        query = "select  video_info.video_name,video_info.like_count, channel_info.name from video_info join channel_info on video_info.chan_id = channel_info.id order by video_info.like_count desc limit 10"
        df = data_frame(query)
        df1 = pd.DataFrame(df, columns=["video_name", "like_count", "channel_name"])
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)
    elif selected == "6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query = "select video_name, like_count, dislike_count from video_info order by like_count desc"
        df = data_frame(query)
        df1 = pd.DataFrame(df, columns=["video_name", "like_count", "dislike_count"])
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)
    elif selected == "7.	What is the total number of views for each channel, and what are their corresponding channel names?":
        query = "select name, view_count from channel_info order by view_count desc"
        df = data_frame(query)
        df1 = pd.DataFrame(df, columns=["channel_name", "total_views"])
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)
    elif selected == "8.	What are the names of all the channels that have published videos in the year 2022?":
        query = "select channel_info.name, count(video_info.video_name) from video_info join channel_info on video_info.chan_id = channel_info.id where extract(year from video_info.published_date) = 2022 group by channel_info.name order by count(video_info.video_name) desc;"
        df = data_frame(query)
        df1 = pd.DataFrame(df, columns=["channel_name", "total_views"])
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)
    elif selected == "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query = "select channel_info.name,avg(video_info.duration) as duration from video_info join channel_info on video_info.chan_id = channel_info.id group by channel_info.name order by duration desc ;"
        df = data_frame(query)
        df1 = pd.DataFrame(df, columns=["channel_name", "avg_duration in mins"])
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)
    elif selected == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        query = "select channel_info.name, video_info.video_name , video_info.comment_count from video_info join channel_info on video_info.chan_id = channel_info.id order by comment_count desc limit 10"
        df = data_frame(query)
        df1 = pd.DataFrame(df, columns=["channel_name", "video_name", "comment_count"])
        df1["comment_count"] = df1["comment_count"].replace(0, np.nan)
        df1.dropna(inplace=True)
        df1.index = [i + 1 for i in range(len(df1))]
        st.dataframe(df1)

if selected_page == "Contact Detail":
    st.write(">>Youtube Scraping:")
    st.write(">>Created by: Priyadharshika.M")
    st.write(">>Linkedin page: https://www.linkedin.com/in/priyadharshika-m-176204269/")
    st.write(">>Github Page: https://github.com/Priyadharshika19")
    st.write(">>Github repository: https://github.com/Priyadharshika19/Youtube_Scraping")


