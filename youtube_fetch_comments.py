import requests
import time
import os, uuid

def fetch_comments(api_key, video_id, output_dir):
    URL = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part" : "snippet",
        "videoId" : video_id,
        "key" : api_key,
        "maxResults" : 20
    }

    comments = []

    response = requests.get(URL, params)
    if response.status_code == 200:
        comments = [
            item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            for item in response.json().get("items", [])
        ]
        # Write comments to a file
        if comments:
            file_path = os.path.join(output_dir, f"{uuid.uuid4()}.txt")
            with open(file_path, "w") as f:
                for comment in comments:
                    f.write(comment + "\n")
    else:
        print(f"Error fetching comments: {response.status_code}")


def simulate_streaming(api_key, video_id, output_dir):
    while True:
        fetch_comments(api_key, video_id, output_dir)
        time.sleep(10)  # Fetch comments every 10 seconds

# VIDEO_URL = "https://www.youtube.com/watch?v=4gulVzzh82g"

API_KEY = "AIzaSyBMYZRIVOvvrjKKIY64bDT3sgMxw15Xqvc"
VIDEO_ID = "4gulVzzh82g"
# comments = fetch_comments(API_KEY, VIDEO_ID)

simulate_streaming(API_KEY, VIDEO_ID, "temp_dir/streaming_dir")
