import requests
import json
from datetime import date

# import os
# from dotenv import load_dotenv
# load_dotenv(dotenv_path="./.env")

from airflow.decorators import task
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50


@task
def get_playlist_id():

    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        response.raise_for_status()

        data = response.json()

        channel_items = data["items"][0]

        channel_playlistID = channel_items["contentDetails"]["relatedPlaylists"]['uploads']

        return channel_playlistID

    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_ids(playlistID):

    video_ids = []

    pageToken = None

    # correct maxResults query parameter
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistID}&key={API_KEY}"

    try:

        while True:
            
            url = base_url

            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            # Loop through each item in the response. the empty list is a default value in case "items" key is not found
            for item in data.get("items", []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            pageToken = data.get('nextPageToken')

            # scenario where there are no more pages to fetch or last page reached
            if not pageToken:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(video_id_list, batch_size):

        for video_id in range(0, len(video_id_list), batch_size):
            yield video_id_list[video_id: video_id + batch_size]

    try :
        for batch in batch_list(video_ids, maxResults):
            video_ids_str= ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewCount": statistics.get('viewCount', None),
                    "likeCount": statistics.get('likeCount', None),
                    "commentCount": statistics.get('commentCount', None),
                }

                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e
    
@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"

    # context manager to handle file operations
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)


# Test the function, where if run directly; __name__ will be set to "__main__"
    # however, if this file is imported as a module in another file, __name__ will be set to the module's name
if __name__ == "__main__":
    playlistID = get_playlist_id()
    video_ids = get_video_ids(playlistID)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)

