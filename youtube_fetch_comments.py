import requests

def fetch_comments(api_key, video_id):
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
        for item in response.json().get("items", []):
            comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            comments.append(comment)
    else:
        print(f"Error : {response.json()}")

    return comments


# VIDEO_URL = "https://www.youtube.com/watch?v=4gulVzzh82g"

# API_KEY = "AIzaSyBMYZRIVOvvrjKKIY64bDT3sgMxw15Xqvc"
# VIDEO_ID = "4gulVzzh82g"
# comments = fetch_comments(API_KEY, VIDEO_ID)