import requests
import pandas as pd
from tqdm import tqdm
import random
import time
import os
from dotenv import load_dotenv

load_dotenv()

# ======================
# CONFIG
# ======================
API_KEY = os.getenv("YT_API_KEY")


# ======================
# FETCH CHANNEL STATS
# ======================
def get_channel_stats(api_key, channel_id):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id={channel_id}&key={api_key}"
    response = requests.get(url).json()

    if not response.get("items"):
        raise ValueError("Invalid channel ID or API key.")

    item = response["items"][0]
    stats = item["statistics"]
    snippet = item["snippet"]

    # profile image (best available)
    thumbs = snippet.get("thumbnails", {}) or {}
    profile_pic = (
        thumbs.get("high", {},).get("url")
        or thumbs.get("medium", {},).get("url")
        or thumbs.get("default", {},).get("url")
        or ""
    )

    return {
        "Channel ID": channel_id,
        "Channel Title": snippet["title"],
        "Description": snippet.get("description", ""),
        "Subscribers": int(stats.get("subscriberCount", 0)),
        "Total Views": int(stats.get("viewCount", 0)),
        "Total Videos": int(stats.get("videoCount", 0)),
        "Profile Picture URL": profile_pic,
    }


# ======================
# GET UPLOADS PLAYLIST ID
# ======================
def get_uploads_playlist_id(api_key, channel_id):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={channel_id}&key={api_key}"
    response = requests.get(url).json()
    return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


# ======================
# FETCH ALL VIDEO IDS
# ======================
def get_video_ids(api_key, playlist_id):
    video_ids = []
    url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlist_id}&key={api_key}"

    while url:
        response = requests.get(url).json()
        for item in response.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])
        url = (
            f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&pageToken={response.get('nextPageToken')}&playlistId={playlist_id}&key={api_key}"
            if "nextPageToken" in response
            else None
        )
        time.sleep(0.1)
    return video_ids


# ======================
# FETCH VIDEO DETAILS
# ======================
def get_video_details(api_key, video_ids):
    videos_data = []
    for i in tqdm(range(0, len(video_ids), 50), desc="Fetching video stats"):
        ids_chunk = ",".join(video_ids[i : i + 50])
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={ids_chunk}&key={api_key}"
        response = requests.get(url).json()

        for item in response.get("items", []):
            snippet = item["snippet"]
            stats = item.get("statistics", {})

            views = int(stats.get("viewCount", 0))
            likes = int(stats.get("likeCount", 0))
            comments = int(stats.get("commentCount", 0))
            engagement_rate = round(((likes + comments) / max(views, 1)) * 100, 2)

            videos_data.append(
                {
                    "Video ID": item["id"],
                    "Title": snippet["title"],
                    "Publish Date": snippet["publishedAt"][:10],
                    "Views": views,
                    "Likes": likes,
                    "Comments": comments,
                    "Engagement Rate (%)": engagement_rate,
                }
            )
        time.sleep(0.1)

    return pd.DataFrame(videos_data)


# ======================
# MOCK GEOGRAPHIC DATA
# ======================
def generate_mock_geo_data(total_views):
    countries = ["US", "IN", "BR", "DE", "GB", "CA", "FR", "PH", "ID", "AU"]
    random_views = [random.randint(1, 100) for _ in countries]
    scale = total_views / max(sum(random_views), 1)
    return (
        pd.DataFrame(
            {"Country": countries, "Views": [int(v * scale) for v in random_views]}
        )
        .sort_values("Views", ascending=False)
        .reset_index(drop=True)
    )


# ======================
# MAIN FUNCTION
# ======================
def get_youtube_data(channel_id: str):
    # Fetch core info
    channel_stats = get_channel_stats(API_KEY, channel_id)
    playlist_id = get_uploads_playlist_id(API_KEY, channel_id)
    video_ids = get_video_ids(API_KEY, playlist_id)
    df_videos = get_video_details(API_KEY, video_ids)

    # Channel-level summary
    df_channel = pd.DataFrame([channel_stats])
    df_channel["Average Views per Video"] = (
        df_channel["Total Views"].iloc[0] / max(int(df_channel["Total Videos"].iloc[0]), 1)
    )
    df_channel["Average Engagement Rate (%)"] = round(
        df_videos["Engagement Rate (%)"].mean(), 2
    )

    # Top videos (by views)
    df_top_videos = (
        df_videos.sort_values("Views", ascending=False).head(10).reset_index(drop=True)
    )

    # Mock geography
    df_geo = generate_mock_geo_data(channel_stats["Total Views"])

    # Return dataframes only (no CSV writes)
    return df_channel, df_videos, df_top_videos, df_geo


if __name__ == "__main__":
    # quick local test
    data = get_youtube_data("UCL0LGQQ-aT5CVsYOhH3tv5Q")
    for name, df in zip(["channel", "videos", "top_videos", "geo"], data):
        print(f"\n{name} preview:")
        print(df.head())
