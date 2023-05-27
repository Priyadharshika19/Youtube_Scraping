

#importing libraries
from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import pymongo
from requests.adapters import ResponseError

#connection
client=pymongo.MongoClient("mongodb+srv://priya:generate@cluster0.m4kzbjb.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_Scraping"]
col=db["Youtube_data"]
#col.delete_many({})

api_key="AIzaSyBA7EXqiRj-61W5o55SoqY4kuXI6FuW1F4"
#api_key = 'AIzaSyCJUmmre2yklwjxI24zpi80jKtc44VNnLs'
#channel_id = 'UCmhRdtdgJCa7mg4Jjv4zAmA'  # tridentinfo   1
#channel_id="UCl5LlCSvu5896s7n1tUYarA"#MATH THE IMMORTAL   2
# channel_id= "UCM3nM3FDTVuO9vpGUYnnupA"   #Jagadesh Anbu   3
# channel_id="UCCEJjddFC2A56TRjAfT4QzQ"# Dreamy Chill Lofi 4
# channel_id = "UCRND_04hdQeL_MWEJs7qj6w"   #5
#channel_id="UCQqmjKQBKQkRbWbSntYJX0Q"     #6 shabarinath
#channel_id="UCQ2Ppkhti0qNU3q7OB1HyRQ"  #7 active learning
#channel_id="UCgLFJc-uJMP-wPZ4LZgnsIQ"   #8 lwm
#channel_id="UChAznrFzAs4EwSQQ3MITRmg"   #9 lwm mob
channel_id="UC94FsWEr9OLhxNLBFKjG-yg"  #10 FOF

youtube = build('youtube', 'v3', developerKey=api_key)


def get_channel_stats(youtube, channel_id):
    # channel details===================================================
    c_id_request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    c_id_response = c_id_request.execute()
    # print(c_id_response)
    info = c_id_response.get("items")[0]

    data = {"channel_detail": {"channel_info": {
        "channel_id": info["id"],
        "channel_name": info["snippet"]["title"],
        "channel_description": info["snippet"]["description"],
        "channel_custom_url": info["snippet"]["customUrl"],
        "channel_published_at": info["snippet"]["publishedAt"][:10],
        "channel_videocount": info["statistics"]["videoCount"],
        "channel_viewcount": info["statistics"]["viewCount"],
        "channel_subscribercount": info["statistics"]["subscriberCount"],
        "channel_playlist_id": info["contentDetails"]["relatedPlaylists"]["uploads"],
    }}}
    df1 = pd.DataFrame(data["channel_detail"])
    # print(df1)
    pl_id = info["contentDetails"]["relatedPlaylists"]["uploads"]
    # video ids=========================================================
    v_request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=pl_id,
        maxResults=50
    )
    # print(p_lst_lst)
    v_response = v_request.execute()
    # print("-----",v_response)
    video_ids = []
    # print(response)
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
                playlistId=pl_id,
                maxResults=50,
                pageToken=next_page_token)
            v_response = request.execute()
            for i in range(len(v_response['items'])):
                video_ids.append(v_response['items'][i]['contentDetails']['videoId'])
            next_page_token = v_response.get('nextPageToken')

    # data["channel_detail"]["video_info"]=video_ids

    # print("=========>",len(video_ids))
    # a=pd.DataFrame(video_ids)
    # print(a)

    # video info============================================================
    all_video_details = []
    for vid in range(0, len(video_ids), 50):
        v_info_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids[vid:vid + 50])
        )
        v_info_response = v_info_request.execute()
        #print("........", v_info_response)

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
                # "duration": video['contentDetails']["duration"]
                # "like_count":  video["statistics"]["likeCount"],
                # "tags": video['snippet']["tags"]
            }
            # print(vid_data)
            try:
                vid_data["duration"] = video['contentDetails']["duration"]
            except:
                vid_data["duration"] = None
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

            #print("###",vid_data)
            all_video_details.append(vid_data)

            # print(all_video_details)
    data["channel_detail"]["video_info"] = all_video_details
    # print(">>>",vid_data)
    # all_video_details.append(video_stats)
    # print(v_info_response)

    # df=pd.DataFrame(data)
    # print(df)

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
            # print(response)
        except:
            pass
        try:
            c_response = response['items'][0]
            # print(len(response))
            # print(len(response['items']))
            cmd_data["comment_id"] = c_response["id"]
            cmd_data["video_id"] = c_response["snippet"]["videoId"]
            # cmd_data["comment_txt"]=response['items'][0]["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            lst = []
            for l in range(0, len(response['items'])):
                a = response['items'][l]["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                lst.append(a)
            cmd_data["comment_txt"] = lst
            cmd_data["comment_author"] = c_response["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
            cmd_data["comment_publish_date"] = c_response["snippet"]["topLevelComment"]["snippet"]["publishedAt"][:10]
            # pritn(len)
            # print(cmd_data)
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
                # print(response)
            except:
                pass
            try:
                c_response = response['items'][0]
                # print(len(response))
                # print(len(response['items']))
                cmd_data["comment_id"] = c_response["id"]
                cmd_data["video_id"] = c_response["snippet"]["videoId"]
                # cmd_data["comment_txt"]=response['items'][0]["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                lst = []
                for l in range(0, len(response['items'])):
                    a = response['items'][l]["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                    lst.append(a)
                cmd_data["comment_txt"] = lst
                cmd_data["comment_author"] = c_response["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                cmd_data["comment_publish_date"] = c_response["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                # pritn(len)
                # print(cmd_data)
                comment_info.append(cmd_data)
            except:
                pass
            next_page_token = response.get('nextPageToken')
    data["channel_detail"]["comment_info"] = comment_info

    col.insert_one(data)
    return data




data = get_channel_stats(youtube, channel_id)

