from flask import Flask, request, jsonify
from get_data import get_youtube_data, get_channel_stats
import requests
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
API_KEY = os.getenv("YT_API_KEY")

app = Flask(__name__)


def make_serializable(val):
    """Convert pandas/numpy/datetime objects to JSON-friendly Python types."""
    # pandas timestamps
    if isinstance(val, (pd.Timestamp, datetime)):
        # keep date format consistent with previous behavior
        try:
            return val.strftime("%Y-%m-%d")
        except Exception:
            return str(val)
    # numpy integer types
    if isinstance(val, (np.integer,)):
        return int(val)
    # numpy float types
    if isinstance(val, (np.floating,)):
        # prefer python float
        return float(val)
    # pandas NA / numpy nan
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    # fallback for other numpy types
    try:
        if hasattr(val, "item"):
            return val.item()
    except Exception:
        pass
    return val


def df_to_json(df):
    # convert DataFrame to serializable JSON; handle datetimes and numpy types
    if df is None:
        return []
    if isinstance(df, (list, dict)):
        return df
    json_df = df.copy()
    # Convert datetimes to YYYY-MM-DD strings
    for c in json_df.columns:
        if pd.api.types.is_datetime64_any_dtype(json_df[c]):
            json_df[c] = json_df[c].dt.strftime("%Y-%m-%d")
    records = json_df.to_dict(orient="records")
    # ensure native python types
    cleaned = []
    for rec in records:
        cleaned_rec = {k: make_serializable(v) for k, v in rec.items()}
        cleaned.append(cleaned_rec)
    return cleaned


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

    try:
        df_channel, df_videos, df_top_videos, df_geo = get_youtube_data(channel_id)
    except Exception as e:
        return {"error": "Failed to fetch data", "details": str(e)}, 500

    return {
        "channel_info": df_to_json(df_channel),
        "videos": df_to_json(df_videos),
        "top_videos": df_to_json(df_top_videos),
        "geo_data": df_to_json(df_geo),
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))  # get port from env or default
    app.run(host="0.0.0.0", port=port)