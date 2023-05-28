import streamlit as st
import psycopg2
import pandas as pd
import numpy as np
db2=psycopg2.connect(host='localhost', user='postgres', password=318327, port=5432, database="yt")
cursor=db2.cursor()

st.set_page_config(page_title="Youtube Scraping",page_icon=":tada",layout="wide")
st.title("YOUTUBE SCRAPING")

def data_frame(q):
    cursor.execute(q)
    my_tuple_list = cursor.fetchall()
    print(my_tuple_list)
    return my_tuple_list


page_bg_img= """
<style>
[data-testid="stAppViewContainer"]
{
text-align: center;
background-color: #cfd3ce;
}  
[data-testid="stHeader"]
{
background-color: #5a83a8;
}
[data-testid="stSidebar"]
{

background-color: #5a83a8;
text-decoration-color: red;
}

</style>
"""
st.markdown(page_bg_img, unsafe_allow_html=True)

selected = st.selectbox("Choose Your Option",(["1. What are the names of all the videos and their corresponding channels?",
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
    query="select channel_info.name, video_info.video_name from video_info join channel_info on video_info.chan_id = channel_info.id order by channel_info.name"
    df = data_frame(query)
    #df.index = [i + 1 for i in range(len(df))]
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
    df1["comment_count"]=df1["comment_count"].replace(0,np.nan)
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
    query ="select name, view_count from channel_info order by view_count desc"
    df = data_frame(query)
    df1 = pd.DataFrame(df, columns=["channel_name", "total_views"])
    df1.index = [i + 1 for i in range(len(df1))]
    st.dataframe(df1)
elif selected == "8.	What are the names of all the channels that have published videos in the year 2022?":
    query ="select channel_info.name, count(video_info.video_name) from video_info join channel_info on video_info.chan_id = channel_info.id where extract(year from video_info.published_date) = 2022 group by channel_info.name order by count(video_info.video_name) desc;"
    df = data_frame(query)
    df1 = pd.DataFrame(df, columns=["channel_name", "total_views"])
    df1.index = [i + 1 for i in range(len(df1))]
    st.dataframe(df1)
elif selected == "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query ="select channel_info.name, video_info.duration  from video_info join channel_info on video_info.chan_id = channel_info.id"
    df = data_frame(query)
    df1 = pd.DataFrame(df, columns=["channel_name", "avg_duration in mins"])
    df1.index = [i + 1 for i in range(len(df1))]
    #st.write(df1["total_duration"])
    st.dataframe(df1)
elif selected == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query ="select channel_info.name, video_info.video_name , video_info.comment_count from video_info join channel_info on video_info.chan_id = channel_info.id order by comment_count desc limit 10"
    df = data_frame(query)
    df1 = pd.DataFrame(df, columns=["channel_name", "video_name", "comment_count"])
    df1["comment_count"] = df1["comment_count"].replace(0, np.nan)
    df1.dropna(inplace=True)
    df1.index = [i + 1 for i in range(len(df1))]
    st.dataframe(df1)