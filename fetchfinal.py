import requests
import json
import aiosqlite
import asyncio


all_videos = []

async def insert_video_info(video_id, channel_name, download_status="not started", download_date=None):
    async with aiosqlite.connect('your_database.db') as db:
        cursor = await db.execute(
            "INSERT OR IGNORE INTO videos (video_id, download_status, download_date, channel_name) VALUES (?, ?, ?, ?)",
            (video_id, download_status, download_date, channel_name)
        )
        await db.commit()

async def main():
    base_url = "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&playlistId={}&key={}"

    googlecloutapitoken = "AIzaSyCXO41GFBSXBr_ArIr7j0y0VQDHHl6LYVA"
    channel_id = "UUJcCB-QYPIBcbKcBQOTwhiA"
    while True:
        url = base_url.format(channel_id, googlecloutapitoken)
        response = requests.get(url, headers={'User-Agent': 'Chrome/80.0.3987.132 Mozilla/5.0'})
        json_value_out = json.loads(response.text)

        items = json_value_out.get("items", [])

        for item in items:
            try:
                video_id = item["snippet"]["resourceId"]["videoId"]
                channel_name = item["snippet"]["channelTitle"]

                await insert_video_info(video_id, channel_name)

            except Exception as e:
                print(e)
                pass

        # Check if there is a next page
        next_page_token = json_value_out.get("nextPageToken")
        if next_page_token:
            base_url += "&pageToken={}".format(next_page_token)
        else:
            break

    # Print the list of all video IDs and channel names
    async with aiosqlite.connect('your_database.db') as db:
        async with db.execute("SELECT * FROM videos") as cursor:
            async for row in cursor:
                print(row)

if __name__ == "__main__":
    asyncio.run(main())
