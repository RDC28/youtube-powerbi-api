import requests
import pandas as pd
from tqdm import tqdm
import random
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import re

load_dotenv()

# ======================
# CONFIG
# ======================
API_KEY = os.getenv("YT_API_KEY")


# ----------------------
# Helpers
# ----------------------
def parse_iso8601_duration(duration: str) -> int:
    """
    Convert ISO 8601 duration (e.g. PT1H2M30S) to total seconds.
    Returns 0 on parse failure.
    """
    if not duration or not isinstance(duration, str):
        return 0
    pattern = re.compile(
        r"PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?"
    )
    m = pattern.match(duration)
    if not m:
        return 0
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)
    return hours * 3600 + minutes * 60 + seconds


def days_between(d1: str, d2: datetime = None) -> int:
    """
    d1: ISO date string 'YYYY-MM-DD' or 'YYYY-MM-DDThh:mm:ssZ'
    returns days between d1 and today (or d2 if provided)
    """
    try:
        if "T" in d1:
            dt = datetime.fromisoformat(d1.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(d1)
    except Exception:
        return 0
    now = d2 or datetime.now(timezone.utc)
    delta = now - dt
    return max(delta.days, 0)


# ======================
# FETCH CHANNEL STATS (extended)
# ======================
def get_channel_stats(api_key, channel_id):
    # request more parts to get publishedAt / branding
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics,contentDetails,brandingSettings&id={channel_id}&key={api_key}"
    response = requests.get(url).json()

    if not response.get("items"):
        raise ValueError("Invalid channel ID or API key.")

    item = response["items"][0]
    stats = item.get("statistics", {}) or {}
    snippet = item.get("snippet", {}) or {}
    branding = item.get("brandingSettings", {}) or {}
    content_details = item.get("contentDetails", {}) or {}

    thumbs = snippet.get("thumbnails", {}) or {}
    profile_pic = (
        (thumbs.get("high") or thumbs.get("medium") or thumbs.get("default") or {}).get(
            "url", ""
        )
    )

    # optional banner image
    banner_url = (
        branding.get("image", {})
        .get("bannerExternalUrl", "")
        if isinstance(branding.get("image", {}), dict)
        else ""
    )

    published_at = snippet.get("publishedAt", None)  # channel creation datetime
    channel_age_days = days_between(published_at) if published_at else None
    channel_age_months = channel_age_days / 30.0 if channel_age_days is not None else None

    total_videos = int(stats.get("videoCount", 0))
    total_views = int(stats.get("viewCount", 0))
    subscribers = int(stats.get("subscriberCount", 0))

    vids_per_month = round(total_videos / max(channel_age_months, 1), 2) if channel_age_months else None
    subs_per_view = round(subscribers / max(total_views, 1), 6)

    return {
        "Channel ID": channel_id,
        "Channel Title": snippet.get("title", ""),
        "Description": snippet.get("description", ""),
        "Country": snippet.get("country", ""),
        "Published At": published_at,
        "Channel Age (days)": channel_age_days,
        "Subscribers": subscribers,
        "Total Views": total_views,
        "Total Videos": total_videos,
        "Videos per Month": vids_per_month,
        "Subscribers per View": subs_per_view,
        "Profile Picture URL": profile_pic,
        "Banner URL": banner_url,
        # include related playlists for possible future use
        "Uploads Playlist ID": content_details.get("relatedPlaylists", {}).get("uploads", ""),
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
# FETCH VIDEO DETAILS (extended)
# ======================
def get_video_details(api_key, video_ids):
    videos_data = []
    # request snippet,statistics,contentDetails,topicDetails (topicDetails might be empty)
    for i in tqdm(range(0, len(video_ids), 50), desc="Fetching video stats"):
        ids_chunk = ",".join(video_ids[i : i + 50])
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails,topicDetails&id={ids_chunk}&key={api_key}"
        response = requests.get(url).json()

        for item in response.get("items", []):
            snippet = item.get("snippet", {}) or {}
            stats = item.get("statistics", {}) or {}
            content = item.get("contentDetails", {}) or {}
            topic = item.get("topicDetails", {}) or {}

            # basic numeric safety
            views = int(stats.get("viewCount", 0))
            likes = int(stats.get("likeCount", 0)) if stats.get("likeCount") is not None else 0
            comments = int(stats.get("commentCount", 0)) if stats.get("commentCount") is not None else 0
            favorite_count = int(stats.get("favoriteCount", 0)) if stats.get("favoriteCount") is not None else 0

            publish_raw = snippet.get("publishedAt", "")
            publish_date = publish_raw[:10] if publish_raw else ""
            days_since_publish = days_between(publish_raw) if publish_raw else 0

            duration_iso = content.get("duration", "")
            duration_seconds = parse_iso8601_duration(duration_iso)

            # rates / derived
            like_rate = round(likes / max(views, 1) * 100, 3)
            comment_rate = round(comments / max(views, 1) * 100, 4)
            engagement_rate = round(((likes + comments) / max(views, 1)) * 100, 3)
            views_per_day = round(views / max(days_since_publish, 1), 2)

            thumbnails = snippet.get("thumbnails", {}) or {}
            thumb_url = (
                (thumbnails.get("maxres") or thumbnails.get("high") or thumbnails.get("medium") or thumbnails.get("default") or {}).get("url", "")
            )

            # tags may be missing
            tags = snippet.get("tags", [])
            tags_joined = ", ".join(tags) if isinstance(tags, list) else ""

            category_id = snippet.get("categoryId", "")

            videos_data.append(
                {
                    "Video ID": item.get("id", ""),
                    "Title": snippet.get("title", ""),
                    "Description": snippet.get("description", ""),
                    "Publish Date": publish_date,
                    "Days Since Publish": days_since_publish,
                    "Duration (ISO8601)": duration_iso,
                    "Duration (seconds)": duration_seconds,
                    "Views": views,
                    "Likes": likes,
                    "Comments": comments,
                    "Favorite Count": favorite_count,
                    "Like Rate (%)": like_rate,
                    "Comment Rate (%)": comment_rate,
                    "Engagement Rate (%)": engagement_rate,
                    "Views per Day": views_per_day,
                    "Thumbnail URL": thumb_url,
                    "Tags": tags_joined,
                    "Category ID": category_id,
                    "Topic IDs": topic.get("topicIds", []),
                }
            )
        time.sleep(0.1)
    df = pd.DataFrame(videos_data)
    # Ensure proper dtypes and add helpful columns for PBI
    if not df.empty:
        df["Publish Date"] = pd.to_datetime(df["Publish Date"], errors="coerce")
        df["Duration (seconds)"] = pd.to_numeric(df["Duration (seconds)"], errors="coerce").fillna(0).astype(int)
        df["Views"] = pd.to_numeric(df["Views"], errors="coerce").fillna(0).astype(int)
        df["Likes"] = pd.to_numeric(df["Likes"], errors="coerce").fillna(0).astype(int)
        df["Comments"] = pd.to_numeric(df["Comments"], errors="coerce").fillna(0).astype(int)
        df["Views per Day"] = pd.to_numeric(df["Views per Day"], errors="coerce").fillna(0)
    return df


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

    # if channel has zero uploads, return empty frames gracefully
    if not video_ids:
        df_videos = pd.DataFrame(
            columns=[
                "Video ID",
                "Title",
                "Description",
                "Publish Date",
                "Days Since Publish",
                "Duration (ISO8601)",
                "Duration (seconds)",
                "Views",
                "Likes",
                "Comments",
                "Favorite Count",
                "Like Rate (%)",
                "Comment Rate (%)",
                "Engagement Rate (%)",
                "Views per Day",
                "Thumbnail URL",
                "Tags",
                "Category ID",
                "Topic IDs",
            ]
        )
    else:
        df_videos = get_video_details(API_KEY, video_ids)

    # Channel-level summary (DataFrame with many useful fields)
    df_channel = pd.DataFrame([channel_stats])
    # safer numeric conversions
    df_channel["Subscribers"] = pd.to_numeric(df_channel["Subscribers"], errors="coerce").fillna(0).astype(int)
    df_channel["Total Views"] = pd.to_numeric(df_channel["Total Views"], errors="coerce").fillna(0).astype(int)
    df_channel["Total Videos"] = pd.to_numeric(df_channel["Total Videos"], errors="coerce").fillna(0).astype(int)
    # derived
    df_channel["Average Views per Video"] = round(df_channel["Total Views"] / df_channel["Total Videos"].replace(0, 1), 2)
    df_channel["Average Engagement Rate (%)"] = round(df_videos["Engagement Rate (%)"].mean() if not df_videos.empty else 0, 3)
    df_channel["Avg Views per Day (channel)"] = round(df_channel["Total Views"] / max(df_channel["Channel Age (days)"].iloc[0], 1), 2) if df_channel["Channel Age (days)"].iloc[0] else None

    # Top videos (by views) — keep top 10
    df_top_videos = df_videos.sort_values("Views", ascending=False).head(10).reset_index(drop=True)

    # Mock geography (keeps same shape)
    df_geo = generate_mock_geo_data(channel_stats["Total Views"])

    # Return dataframes only (no CSV writes)
    return df_channel, df_videos, df_top_videos, df_geo


if __name__ == "__main__":
    # quick local test
    data = get_youtube_data("UCL0LGQQ-aT5CVsYOhH3tv5Q")
    for name, df in zip(["channel", "videos", "top_videos", "geo"], data):
        print(f"\n{name} preview:")
        print(df.head())