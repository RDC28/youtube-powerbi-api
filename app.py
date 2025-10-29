from flask import Flask, request, jsonify
from get_data import get_youtube_data, get_channel_stats
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("YT_API_KEY")

app = Flask(__name__)

# --- helper to convert DataFrames safely ---
def df_to_json(df):
    return df.to_dict(orient="records")


@app.route("/")
def home():
    return {"message": "YouTube Data API is running 🚀"}


@app.route("/api/channel_id", methods=["GET"])
def get_channel_id():
    channel_name = request.args.get("channel_name")
    if not channel_name:
        return {"error": "Missing channel_name parameter"}, 400

    url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&q={channel_name}&key={API_KEY}"
    response = requests.get(url).json()

    if "items" not in response or len(response["items"]) == 0:
        return {"error": "Channel not found"}, 404

    channel = response["items"][0]
    return {
        "channel_name": channel["snippet"]["title"],
        "channel_id": channel["id"]["channelId"],
    }


@app.route("/api/data", methods=["GET"])
def get_data():
    channel_name = request.args.get("channel_name")
    channel_id = request.args.get("channel_id")

    # If channel name is given, find its ID automatically
    if channel_name and not channel_id:
        url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&q={channel_name}&key={API_KEY}"
        res = requests.get(url).json()
        if "items" not in res or len(res["items"]) == 0:
            return {"error": "Channel not found"}, 404
        channel_id = res["items"][0]["id"]["channelId"]

    if not channel_id:
        return {"error": "Missing channel_id or channel_name parameter"}, 400

    df_channel, df_videos, df_top_videos, df_geo = get_youtube_data(channel_id)
    return {
        "channel_info": df_to_json(df_channel),
        "videos": df_to_json(df_videos),
        "top_videos": df_to_json(df_top_videos),
        "geo_data": df_to_json(df_geo),
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))  # get port from env or default
    app.run(host="0.0.0.0", port=port)