import yt_dlp
import asyncio
import aiosqlite
from datetime import datetime
from aiohttp import ClientSession

# Global variables
GLOBAL_QUALITY = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]'
PARALLEL_LIMIT = 5
DOWNLOAD_PATH = '/Users/manojkumar/Documents/secondworkspace/fiverr1/youtubedownloadingprojectfinal/output'

async def download_video(video_id, db_conn, session):
    yt_opts = {
        'verbose': True,
        'force_keyframes_at_cuts': True,
        'format': GLOBAL_QUALITY,
        'outtmpl': f'{DOWNLOAD_PATH}/%(title)s.%(ext)s',
    }

    try:
        async with session.get(f'https://www.youtube.com/watch?v={video_id}') as response:
            info_dict = await asyncio.to_thread(yt_dlp.YoutubeDL(yt_opts).extract_info, f'https://www.youtube.com/watch?v={video_id}', download=False)
            if info_dict.get('formats'):
                try:
                    await asyncio.to_thread(yt_dlp.YoutubeDL(yt_opts).download, [f'https://www.youtube.com/watch?v={video_id}'])
                except yt_dlp.DownloadError:
                    print(f"Specified quality {GLOBAL_QUALITY} not available for video {video_id}. Downloading the best available quality.")
                    yt_opts['format'] = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]'
                    await asyncio.to_thread(yt_dlp.YoutubeDL(yt_opts).download, [f'https://www.youtube.com/watch?v={video_id}'])

                download_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                await db_conn.execute(f"UPDATE videos SET download_status='downloaded', download_date=? WHERE video_id=?", (download_date, video_id))
                await db_conn.commit()
            else:
                print(f"No available MP4 formats for video {video_id}. Skipping.")
                await db_conn.execute(f"UPDATE videos SET download_status='skipped' WHERE video_id=?", (video_id,))
                await db_conn.commit()

    except yt_dlp.DownloadError as e:
        print(f"Error downloading video {video_id}: {e}")
        await db_conn.execute(f"UPDATE videos SET download_status='error' WHERE video_id=?", (video_id,))
        await db_conn.commit()

async def download_videos(videos_to_download, db_conn):
    tasks = []
    async with ClientSession() as session:
        for video_id, in videos_to_download:
            task = download_video(video_id, db_conn, session)
            tasks.append(task)
            if len(tasks) >= PARALLEL_LIMIT:
                await asyncio.gather(*tasks)
                tasks = []

        if tasks:
            await asyncio.gather(*tasks)

async def main():
    db_conn = await aiosqlite.connect('your_database.db')
    await db_conn.execute('''CREATE TABLE IF NOT EXISTS videos (video_id TEXT PRIMARY KEY, download_status TEXT, download_date TEXT)''')
    await db_conn.commit()

    videos_to_download = await db_conn.execute_fetchall("SELECT video_id FROM videos  WHERE download_status != 'downloaded'")
    await download_videos(videos_to_download, db_conn)

    await db_conn.close()

if __name__ == "__main__":
    asyncio.run(main())
