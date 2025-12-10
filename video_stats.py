import requests
import json

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
maxResults = 50

def get_playlist_id():

    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        response.raise_for_status()

        data = response.json()

        # print(json.dumps(data, indent=4))

        channel_items = data["items"][0]

        channel_playlistID = channel_items["contentDetails"]["relatedPlaylists"]['uploads']

        # print(channel_playlistID)

        return channel_playlistID

    except requests.exceptions.RequestException as e:
        raise e
    
playlistID = get_playlist_id()

def get_video_ids(playlistID):

    video_ids = []

    pageToken = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&{maxResults}=1&playlistId={playlistID}&key={API_KEY}"

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


# Test the function, where if run directly; __name__ will be set to "__main__"
    # however, if this file is imported as a module in another file, __name__ will be set to the module's name
if __name__ == "__main__":
    playlistID = get_playlist_id()
    print(get_video_ids(playlistID))

