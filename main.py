import os
import re
import logging
import asyncio
import tempfile
import aiohttp
from typing import Optional, Tuple, Dict, List

# Telethon Imports
from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeAudio

# Speed Check for Telethon
try:
    import cryptg
    FAST_MODE = True
except ImportError:
    FAST_MODE = False

# Mutagen Imports
from mutagen import File as MutagenFile
from mutagen.mp3 import MP3
from mutagen.id3 import APIC, TIT2
from mutagen.mp4 import MP4, MP4Cover
from mutagen.flac import FLAC, Picture
from mutagen.oggvorbis import OggVorbis

# --- Logging Configuration ---
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("ConcurrentFastTaggerBot")

if FAST_MODE:
    logger.info("⚡ cryptg is installed! Fast Download & Upload ENABLED.")
else:
    logger.warning("⚠️ cryptg is NOT installed! Download & Upload will be SLOW. Please run 'pip install cryptg'.")

# --- Environment Variables ---
API_ID = 4402984
API_HASH = "aa95b6e3675cbda608ea311be9258d9b"
BOT_TOKEN = "8633858088:AAH48E3Li_FqXKzq_4cFfByWxUaZMNwWSgI"

ALLOWED_EXTENSIONS = {'.mp3', '.m4a', '.mp4', '.ogg', '.flac'}

# --- Memory Storage ---
user_queues: Dict[int, List[events.NewMessage.Event]] = {}
processing_locks: Dict[int, asyncio.Lock] = {}
last_episode_numbers: Dict[int, int] = {}

# Concurrent Limit (एक साथ कितनी फाइल डाउनलोड करनी है)
MAX_CONCURRENT_DOWNLOADS = 5
semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

def get_chat_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in processing_locks:
        processing_locks[chat_id] = asyncio.Lock()
    return processing_locks[chat_id]

def extract_number_from_text(text: str) -> Optional[int]:
    if not text:
        return None
    match = re.search(r'\d+', text)
    return int(match.group()) if match else None

# --- Helpers ---
def get_image_mime_and_format(data: bytes) -> Tuple[str, int]:
    if data.startswith(b'\xff\xd8'):
        return 'image/jpeg', MP4Cover.FORMAT_JPEG
    elif data.startswith(b'\x89PNG\r\n\x1a\n'):
        return 'image/png', MP4Cover.FORMAT_PNG
    return 'image/jpeg', MP4Cover.FORMAT_JPEG

def get_audio_info(file_path: str) -> Tuple[int, str]:
    duration = 0
    raw_title = ""
    try:
        audio = MutagenFile(file_path)
        if audio is not None:
            if hasattr(audio.info, 'length'):
                duration = int(audio.info.length)
    except Exception as e:
        logger.error(f"Failed to read audio info: {e}")
    return duration, raw_title

# --- Image Downloaders ---
async def download_image_from_url(session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200 and 'image' in response.headers.get('content-type', '').lower():
                return await response.read()
    except Exception as e:
        logger.error(f"Error fetching direct image URL: {e}")
    return None

async def download_image_from_tg(client, url: str) -> Optional[bytes]:
    match = re.match(r'https?://t\.me/(?:c/)?([^/]+)/(\d+)', url)
    if not match:
        return None
    try:
        channel_ref = match.group(1)
        message_id = int(match.group(2))
        if channel_ref.isdigit():
            channel_ref = int(f"-100{channel_ref}") if not channel_ref.startswith("-100") else int(channel_ref)
        
        try:
            entity = await client.get_entity(channel_ref)
        except Exception:
            entity = channel_ref
        
        msg = await client.get_messages(entity, ids=message_id)
        if not msg:
            return None
        
        if msg.photo:
            return await client.download_media(msg.photo, bytes)
        elif msg.document and msg.document.mime_type and msg.document.mime_type.startswith('image/'):
            return await client.download_media(msg.document, bytes)
        elif msg.media and hasattr(msg.media, 'webpage') and msg.media.webpage and getattr(msg.media.webpage, 'photo', None):
            return await client.download_media(msg.media.webpage.photo, bytes)
    except Exception as e:
        logger.error(f"Error resolving Telegram link: {e}")
    return None

# --- Sync Attacher for Thread Pool ---
def _attach_image_and_title_sync(file_path: str, image_data: bytes, title: str) -> bool:
    try:
        audio = MutagenFile(file_path)
        if audio is None:
            return False

        mime_type, img_format = get_image_mime_and_format(image_data)

        if isinstance(audio, MP3):
            if audio.tags is None:
                audio.add_tags()
            audio.tags.add(TIT2(encoding=3, text=title))
            keys_to_delete = [k for k in audio.tags.keys() if k.startswith("APIC")]
            for key in keys_to_delete:
                audio.tags.pop(key, None)
            audio.tags.add(APIC(encoding=3, mime=mime_type, type=3, desc='Cover', data=image_data))
            audio.save()
            return True

        elif isinstance(audio, MP4):
            audio["\xa9nam"] = [title]
            audio["covr"] = [MP4Cover(image_data, imageformat=img_format)]
            audio.save()
            return True

        elif isinstance(audio, FLAC):
            audio['title'] = [title]
            pic = Picture()
            pic.mime = mime_type
            pic.type = 3
            pic.desc = 'Cover'
            pic.data = image_data
            audio.add_picture(pic)
            audio.save()
            return True

        elif isinstance(audio, OggVorbis):
            audio['title'] = [title]
            audio.save()
            return True

        return False
    except Exception as e:
        logger.error(f"Error attaching image/title: {e}")
        return False

# --- Concurrent Worker Function ---
async def process_single_file_task(client, http_session, temp_dir, index, msg_event, final_title):
    async with semaphore:  # Limit concurrent downloads to avoid ban/flood
        try:
            caption = msg_event.text or ""
            url_match = re.search(r'https?://[^\s]+', caption)
            image_url = url_match.group(0) if url_match else ""

            if not image_url:
                return index, None, None, 0, final_title, "No image link"

            # Download Image
            if "t.me/" in image_url:
                image_data = await download_image_from_tg(client, image_url)
            else:
                image_data = await download_image_from_url(http_session, image_url)

            if not image_data:
                return index, None, None, 0, final_title, "Image download failed"

            # Download Audio
            file_ext = msg_event.file.ext or ".mp3"
            audio_path = os.path.join(temp_dir, f"audio_{index}{file_ext}")
            
            await client.download_media(msg_event.media, audio_path)

            duration, _ = await asyncio.to_thread(get_audio_info, audio_path)

            # Attach Cover & Title
            success = await asyncio.to_thread(_attach_image_and_title_sync, audio_path, image_data, final_title)
            if not success:
                return index, None, None, 0, final_title, "Tagging failed"

            thumb_path = os.path.join(temp_dir, f"thumb_{index}.jpg")
            with open(thumb_path, "wb") as f:
                f.write(image_data)

            return index, audio_path, thumb_path, duration, final_title, "Success"
        except Exception as e:
            logger.error(f"Error processing file {index}: {e}")
            return index, None, None, 0, final_title, str(e)

# --- Telegram Bot Setup ---
bot = TelegramClient('image_tagger_bot', API_ID, API_HASH)

@bot.on(events.NewMessage(incoming=True, pattern=r'^/(start|process)$'))
async def command_handler(event):
    chat_id = event.chat_id
    queue = user_queues.get(chat_id, [])

    if not queue:
        await event.respond(
            "👋 **नमस्ते!**\n\n"
            "पहले अपनी सभी ऑडियो फाइल्स भेजें।\n"
            "जब सारी फाइल्स भेज दें, तो **/process** कमांड भेजें!"
        )
        return

    chat_lock = get_chat_lock(chat_id)
    if chat_lock.locked():
        await event.respond("⚠️ प्रोसेसिंग पहले से चल रही है...")
        return

    async with chat_lock:
        status_msg = await event.respond(f"🚀 **{len(queue)} फाइल्स एक साथ (Concurrently) डाउनलोड हो रही हैं... कृपया प्रतीक्षा करें।**")
        files_to_process = list(queue)
        user_queues[chat_id] = []

        # 1. Pre-calculate all Titles Sequentially to maintain episode numbering perfectly
        file_metadata = []
        for index, msg_event in enumerate(files_to_process, start=1):
            orig_title = ""
            if msg_event.document and msg_event.document.attributes:
                for attr in msg_event.document.attributes:
                    if isinstance(attr, DocumentAttributeAudio) and attr.title:
                        orig_title = attr.title

            raw_filename = os.path.splitext(msg_event.file.name)[0] if msg_event.file.name else ""

            found_num = extract_number_from_text(orig_title) or extract_number_from_text(raw_filename)
            
            if found_num is not None:
                final_title = f"Ep - {found_num}"
                last_episode_numbers[chat_id] = found_num
            else:
                prev_ep = last_episode_numbers.get(chat_id, 0)
                next_ep = prev_ep + 1
                final_title = f"Ep - {next_ep}"
                last_episode_numbers[chat_id] = next_ep
            
            file_metadata.append((index, msg_event, final_title))

        # 2. Process all files concurrently
        async with aiohttp.ClientSession() as http_session:
            with tempfile.TemporaryDirectory() as temp_dir:
                
                # Create background tasks for all files
                tasks = [
                    process_single_file_task(bot, http_session, temp_dir, idx, msg, f_title)
                    for idx, msg, f_title in file_metadata
                ]
                
                # Run all tasks at once! (Gather preserves the order of results)
                results = await asyncio.gather(*tasks)

                await status_msg.edit(f"⏳ **डाउनलोड और प्रोसेसिंग पूरी हुई! अब सही क्रम में अपलोड किया जा रहा है...**")

                # 3. Upload sequentially in exact original order
                for res in results:
                    idx, audio_path, thumb_path, duration, final_title, status = res
                    
                    if status == "Success" and audio_path:
                        await bot.send_file(
                            chat_id,
                            file=audio_path,
                            caption=f"✅ **{final_title}** तैयार!",
                            thumb=thumb_path,
                            attributes=[
                                DocumentAttributeAudio(
                                    duration=duration,
                                    title=final_title,
                                    performer="Custom Cover"
                                )
                            ]
                        )
                    else:
                        await event.respond(f"❌ **{final_title}** विफल हुई: {status}")

        await status_msg.edit("🎉 **सभी फाइल्स सही क्रम (Sequentially) में सफलतापूर्वक अपलोड हो गई हैं!**")

@bot.on(events.NewMessage(incoming=True))
async def collect_audio_files(event):
    if event.text and event.text.startswith('/'):
        return

    if not event.media or not (event.voice or event.audio or (event.document and any(event.file.name.endswith(ext) for ext in ALLOWED_EXTENSIONS if event.file.name))):
        return

    caption = event.text or ""
    if not re.search(r'https?://[^\s]+', caption):
        await event.respond("⚠️ **कृपया कैप्शन में इमेज लिंक शामिल करें!**")
        return

    chat_id = event.chat_id
    if chat_id not in user_queues:
        user_queues[chat_id] = []

    user_queues[chat_id].append(event)

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("Concurrent Download & Sequential Upload Bot Running...")
    await bot.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
        
