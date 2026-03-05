"""
TG Session Manager - Backend Server with Verifier
"""

import os
import json
import asyncio
import random
import re
import uuid
import httpx
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
from collections import defaultdict

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from pydantic import BaseModel
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

VERSION_APP = "2.1"

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")  # Main bot token from .env

SESSIONS_DIR = Path(os.getenv("TG_SESSIONS_DIR", "/opt/imitation_chat/sessions"))
CACHE_DIR = Path(os.getenv("TG_CACHE_DIR", "/opt/imitation_chat/cache"))
PHOTOS_DIR = CACHE_DIR / "photos"
TEXTS_DIR = Path(os.getenv("TG_TEXTS_DIR", "/opt/imitation_chat"))
VERIFIER_DIR = CACHE_DIR / "verifier"

USERS_FILE = CACHE_DIR / "users.json"
SESSIONS_FILE = CACHE_DIR / "sessions.json"
GROUPS_FILE = CACHE_DIR / "groups.json"

SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
VERIFIER_DIR.mkdir(parents=True, exist_ok=True)

# Active tasks
active_chats: Dict[str, asyncio.Task] = {}
chat_status: Dict[str, dict] = {}
completed_groups: set = set()
deletion_tasks: Dict[str, asyncio.Task] = {}
verifier_tasks: Dict[str, asyncio.Task] = {}

# Track last verification messages per group to delete them
last_verify_messages: Dict[str, int] = {}  # group_id -> message_id

app = FastAPI(title="TG Session Manager API")


# === Models ===
class UserCreate(BaseModel):
    firstName: str = ""
    lastName: str = ""
    username: str = ""
    bio: str = ""


class UserUpdate(BaseModel):
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    username: Optional[str] = None
    bio: Optional[str] = None


class SessionAssign(BaseModel):
    userId: Optional[str] = None


class BotAssign(BaseModel):
    botName: str
    userId: Optional[str] = None


class VerifierSettings(BaseModel):
    enabled: bool = False
    botToken: str = ""
    botUsername: str = ""
    messageText: str = "Для отправки сообщений пройдите верификацию"
    buttonText: str = "Верификация"


class GroupSettings(BaseModel):
    lineRangeStart: int = 1
    lineRangeEnd: int = 100
    dayPauseMin: int = 3
    dayPauseMax: int = 10
    nightPauseMin: int = 50
    nightPauseMax: int = 100
    randomPauseChance: int = 10
    randomPauseMin: int = 21
    randomPauseMax: int = 30
    nightStartHour: int = 23
    nightEndHour: int = 7
    inviteLink: str = ""
    botAssignments: Dict[str, str] = {}
    deleteSystemMessages: bool = False
    verifier: dict = {
        "enabled": False,
        "botToken": "",
        "botUsername": "",
        "messageText": "Для отправки сообщений пройдите верификацию",
        "buttonText": "Верификация"
    }


# === Data helpers ===
def load_json(path: Path) -> list:
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return []


def save_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_users() -> List[dict]:
    return load_json(USERS_FILE)


def save_users(users: List[dict]):
    save_json(USERS_FILE, users)


def load_sessions_meta() -> List[dict]:
    return load_json(SESSIONS_FILE)


def save_sessions_meta(sessions: List[dict]):
    save_json(SESSIONS_FILE, sessions)


def load_groups() -> List[dict]:
    return load_json(GROUPS_FILE)


def save_groups(groups: List[dict]):
    save_json(GROUPS_FILE, groups)


def sync_sessions():
    meta = load_sessions_meta()
    meta_ids = {m["id"] for m in meta}
    
    for session_file in SESSIONS_DIR.glob("*.session"):
        session_id = session_file.stem
        if session_id not in meta_ids:
            meta.append({
                "id": session_id,
                "filename": session_file.name,
                "phone": "",
                "userId": None,
                "isAuthorized": True,
                "uploadedAt": datetime.now().isoformat()
            })
    
    existing_files = {f.stem for f in SESSIONS_DIR.glob("*.session")}
    meta = [m for m in meta if m["id"] in existing_files]
    
    save_sessions_meta(meta)
    return meta


def parse_text_file(filepath: Path) -> List[dict]:
    messages = []
    if not filepath.exists():
        logger.warning(f"Text file not found: {filepath}")
        return messages
    
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    
    lines = content.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    
    line_num = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        match = re.match(r'^([^:]+):<(.+)>$', line)
        if match:
            line_num += 1
            messages.append({
                "line": line_num,
                "name": match.group(1).strip(),
                "text": match.group(2).strip()
            })
    
    return messages


def get_bots_from_text() -> List[dict]:
    text_file = TEXTS_DIR / "text_bad_1.txt"
    messages = parse_text_file(text_file)
    
    if not messages:
        return []
    
    bot_messages = defaultdict(int)
    bot_order = {}
    
    for msg in messages:
        name = msg["name"]
        bot_messages[name] += 1
        if name not in bot_order:
            bot_order[name] = len(bot_order)
    
    bots = []
    for name, count in bot_messages.items():
        bots.append({
            "name": name,
            "ordinal": bot_order[name],
            "messagesCount": count,
            "userId": None
        })
    
    return sorted(bots, key=lambda b: b["ordinal"])


def get_group_bots(group_id: str) -> List[dict]:
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    
    bots = get_bots_from_text()
    
    if group and "settings" in group:
        assignments = group["settings"].get("botAssignments", {})
        for bot in bots:
            bot["userId"] = assignments.get(bot["name"])
    
    return bots


# === Verifier Functions ===
def get_verified_file(group_id: str) -> Path:
    return VERIFIER_DIR / f"{group_id}.json"


def load_verified_users(group_id: str) -> set:
    filepath = get_verified_file(group_id)
    if filepath.exists():
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                return set(data.get("users", []))
        except:
            pass
    return set()


def save_verified_users(group_id: str, users: set):
    filepath = get_verified_file(group_id)
    with open(filepath, "w") as f:
        json.dump({"users": list(users)}, f)


def is_user_verified(group_id: str, user_id: int) -> bool:
    return user_id in load_verified_users(group_id)


def verify_user(group_id: str, user_id: int):
    users = load_verified_users(group_id)
    users.add(user_id)
    save_verified_users(group_id, users)
    logger.info(f"[{group_id}] User {user_id} verified")


async def auto_verify_sessions(group_id: str, settings: GroupSettings):
    """Auto-verify all telethon sessions"""
    logger.info(f"[{group_id}] Auto-verifying sessions...")
    
    sessions = load_sessions_meta()
    bot_assignments = settings.botAssignments or {}
    assigned_user_ids = set(bot_assignments.values())
    
    verified_count = 0
    
    for session in sessions:
        if not session.get("userId"):
            continue
        if session["userId"] not in assigned_user_ids:
            continue
        
        session_id = session["id"]
        session_path = SESSIONS_DIR / f"{session_id}.session"
        
        if not session_path.exists():
            continue
        
        try:
            from telethon import TelegramClient
            
            client = TelegramClient(str(session_path.with_suffix('')), int(API_ID), API_HASH)
            await client.connect()
            
            if await client.is_user_authorized():
                me = await client.get_me()
                if me:
                    verify_user(group_id, me.id)
                    verified_count += 1
            
            await client.disconnect()
        except Exception as e:
            logger.error(f"[{group_id}] Error verifying session {session_id}: {e}")
    
    logger.info(f"[{group_id}] Auto-verified {verified_count} sessions")


async def mute_user(bot_token: str, chat_id: int, user_id: int):
    """Mute user using bot API"""
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{bot_token}/restrictChatMember",
                json={
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "permissions": {
                        "can_send_messages": False,
                        "can_send_audios": False,
                        "can_send_documents": False,
                        "can_send_photos": False,
                        "can_send_videos": False,
                        "can_send_video_notes": False,
                        "can_send_voice_notes": False,
                        "can_send_polls": False,
                        "can_send_other_messages": False,
                        "can_add_web_page_previews": False
                    }
                }
            )
            logger.info(f"[{chat_id}] Muted user {user_id}")
    except Exception as e:
        logger.error(f"[{chat_id}] Failed to mute user {user_id}: {e}")


async def unmute_user(bot_token: str, chat_id: int, user_id: int):
    """Unmute user using bot API"""
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{bot_token}/restrictChatMember",
                json={
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "permissions": {
                        "can_send_messages": True,
                        "can_send_audios": True,
                        "can_send_documents": True,
                        "can_send_photos": True,
                        "can_send_videos": True,
                        "can_send_video_notes": True,
                        "can_send_voice_notes": True,
                        "can_send_polls": True,
                        "can_send_other_messages": True,
                        "can_add_web_page_previews": True
                    }
                }
            )
            logger.info(f"[{chat_id}] Unmuted user {user_id}")
    except Exception as e:
        logger.error(f"[{chat_id}] Failed to unmute user {user_id}: {e}")


async def delete_message(bot_token: str, chat_id: int, message_id: int):
    """Delete message using bot API"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"https://api.telegram.org/bot{bot_token}/deleteMessage",
                json={"chat_id": chat_id, "message_id": message_id}
            )
            result = response.json()
            if result.get("ok"):
                logger.info(f"[{chat_id}] Deleted message {message_id}")
            else:
                logger.warning(f"[{chat_id}] Failed to delete message {message_id}: {result}")
    except Exception as e:
        logger.error(f"[{chat_id}] Error deleting message {message_id}: {e}")


async def send_verify_message(bot_token: str, chat_id: int, text: str, button_text: str, bot_username: str) -> Optional[int]:
    """Send verification message with button, returns message_id"""
    if not bot_username:
        return None
    
    # Build bot link from username
    bot_link = f"https://t.me/{bot_username.replace('@', '')}"
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "disable_notification": True,
                    "reply_markup": {
                        "inline_keyboard": [[{
                            "text": button_text,
                            "url": bot_link
                        }]]
                    }
                }
            )
            data = response.json()
            if data.get("ok"):
                return data["result"]["message_id"]
    except Exception as e:
        logger.error(f"[{chat_id}] Failed to send verify message: {e}")
    return None


# === Verifier Bot Webhook ===
@app.post("/verifier-webhook/{group_id}")
async def verifier_bot_webhook(group_id: str, request: Request):
    """Webhook for the verifier bot - handles /start in private chat for verification confirmation"""
    try:
        data = await request.json()
        logger.info(f"[verifier-{group_id}] Received webhook: {json.dumps(data)[:500]}")
        
        if "message" in data:
            message = data["message"]
            chat = message.get("chat", {})
            user = message.get("from", {})
            text = message.get("text", "")
            chat_type = chat.get("type", "")
            user_id = user.get("id")
            
            logger.info(f"[verifier-{group_id}] Message from user {user_id} in chat type {chat_type}: {text[:50] if text else '<no text>'}")
            
            # Handle /start command ONLY in private chat
            if chat_type == "private" and text and text.startswith("/start"):
                if user_id:
                    logger.info(f"[verifier-{group_id}] Private /start from user {user_id}, verifying...")
                    
                    # Verify the user
                    verify_user(group_id, user_id)
                    
                    # Get group settings
                    groups = load_groups()
                    group = next((g for g in groups if g["id"] == group_id), None)
                    
                    if group:
                        verifier = group.get("settings", {}).get("verifier", {})
                        verifier_token = verifier.get("botToken", "")
                        
                        logger.info(f"[verifier-{group_id}] verifier.enabled={verifier.get('enabled')}, BOT_TOKEN={'set' if BOT_TOKEN else 'not set'}")
                        
                        if verifier.get("enabled"):
                            # Unmute using main bot (BOT_TOKEN) - the one that sits in the group
                            if BOT_TOKEN:
                                logger.info(f"[verifier-{group_id}] Unmuting user {user_id} using main bot")
                                await unmute_user(BOT_TOKEN, int(group_id), user_id)
                            else:
                                logger.warning(f"[verifier-{group_id}] BOT_TOKEN not set, cannot unmute")
                            
                            # Send confirmation using verifier bot token
                            if verifier_token:
                                async with httpx.AsyncClient() as client:
                                    await client.post(
                                        f"https://api.telegram.org/bot{verifier_token}/sendMessage",
                                        json={
                                            "chat_id": user_id,
                                            "text": "✅ Верификация пройдена! Теперь вы можете писать в группу."
                                        }
                                    )
                                logger.info(f"[verifier-{group_id}] Sent confirmation to user {user_id}")
                    else:
                        logger.warning(f"[verifier-{group_id}] Group not found")
            
            # Ignore group messages on this webhook - they go to main /webhook
        
        return {"ok": True}
    except Exception as e:
        logger.error(f"Verifier webhook error: {e}", exc_info=True)
        return {"ok": False}


# === Main Bot Webhook (handles group messages) ===
@app.post("/webhook")
async def bot_webhook(request: Request):
    try:
        data = await request.json()
        logger.debug(f"[webhook] Received: {json.dumps(data)[:500]}")
        
        # Handle bot added/removed from group
        if "my_chat_member" in data:
            update = data["my_chat_member"]
            chat = update.get("chat", {})
            new_member = update.get("new_chat_member", {})
            
            chat_id = str(chat.get("id", ""))
            chat_title = chat.get("title", "Unknown")
            status = new_member.get("status", "")
            
            groups = load_groups()
            
            if status in ["member", "administrator"]:
                if not any(g["id"] == chat_id for g in groups):
                    groups.append({
                        "id": chat_id,
                        "chatId": chat_id,
                        "title": chat_title,
                        "username": chat.get("username"),
                        "membersCount": 0,
                        "photoUrl": None,
                        "isRunning": False,
                        "settings": GroupSettings().dict()
                    })
                    save_groups(groups)
                    logger.info(f"Added group: {chat_title}")
            elif status in ["left", "kicked"]:
                groups = [g for g in groups if g["id"] != chat_id]
                save_groups(groups)
                logger.info(f"Removed group: {chat_title}")
        
        # Handle group messages for verification
        if "message" in data:
            message = data["message"]
            chat = message.get("chat", {})
            user = message.get("from", {})
            
            chat_type = chat.get("type", "")
            if chat_type in ["group", "supergroup"]:
                chat_id = str(chat.get("id", ""))
                user_id = user.get("id")
                message_id = message.get("message_id")
                
                # Check if this is a system message (user joined)
                if message.get("new_chat_members") or message.get("left_chat_member"):
                    logger.info(f"[{chat_id}] System message detected: new_chat_members={bool(message.get('new_chat_members'))}, left_chat_member={bool(message.get('left_chat_member'))}")
                    groups = load_groups()
                    group = next((g for g in groups if g["id"] == chat_id), None)
                    if group:
                        delete_enabled = group.get("settings", {}).get("deleteSystemMessages", False)
                        logger.info(f"[{chat_id}] deleteSystemMessages={delete_enabled}, BOT_TOKEN={'set' if BOT_TOKEN else 'not set'}")
                        if delete_enabled and BOT_TOKEN:
                            logger.info(f"[{chat_id}] Deleting system message {message_id}")
                            await delete_message(BOT_TOKEN, int(chat_id), message_id)
                    else:
                        logger.warning(f"[{chat_id}] Group not found in database")
                    return {"ok": True}
                
                # Check verification
                groups = load_groups()
                group = next((g for g in groups if g["id"] == chat_id), None)
                
                if group:
                    verifier = group.get("settings", {}).get("verifier", {})
                    verifier_enabled = verifier.get("enabled", False)
                    
                    logger.debug(f"[{chat_id}] Message from user {user_id}, verifier_enabled={verifier_enabled}")
                    
                    if verifier_enabled and user_id and BOT_TOKEN:
                        # Skip bots
                        if user.get("is_bot"):
                            logger.debug(f"[{chat_id}] Skipping bot user {user_id}")
                            return {"ok": True}
                        
                        # Check if user is verified
                        user_verified = is_user_verified(chat_id, user_id)
                        logger.info(f"[{chat_id}] User {user_id} verified={user_verified}")
                        
                        if not user_verified:
                            logger.info(f"[{chat_id}] User {user_id} not verified, deleting message and muting")
                            
                            # Delete their message
                            await delete_message(BOT_TOKEN, int(chat_id), message_id)
                            
                            # Mute user
                            await mute_user(BOT_TOKEN, int(chat_id), user_id)
                            
                            # Delete previous verification message if exists
                            prev_msg_id = last_verify_messages.get(chat_id)
                            if prev_msg_id:
                                await delete_message(BOT_TOKEN, int(chat_id), prev_msg_id)
                            
                            # Send new verification message
                            bot_username = verifier.get("botUsername", "")
                            logger.info(f"[{chat_id}] Sending verify message, botUsername={bot_username}")
                            
                            new_msg_id = await send_verify_message(
                                BOT_TOKEN,
                                int(chat_id),
                                verifier.get("messageText", "Пройдите верификацию"),
                                verifier.get("buttonText", "Верификация"),
                                bot_username
                            )
                            
                            if new_msg_id:
                                last_verify_messages[chat_id] = new_msg_id
                                logger.info(f"[{chat_id}] Sent verify message {new_msg_id}")
                            else:
                                logger.warning(f"[{chat_id}] Failed to send verify message")
        
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return {"ok": False}


@app.post("/setup-webhook")
async def setup_webhook():
    if not BOT_TOKEN:
        return {"success": False, "error": "BOT_TOKEN not set"}
    
    webhook_url = os.getenv("WEBHOOK_URL", "https://own-zone.ru/tg-util/api/webhook")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            json={"url": webhook_url, "allowed_updates": ["my_chat_member", "message", "chat_member"]}
        )
        return {"success": True, "result": response.json()}


@app.post("/setup-verifier-webhook/{group_id}")
async def setup_verifier_webhook(group_id: str):
    """Setup webhook for the verifier bot"""
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    
    if not group:
        return {"success": False, "error": "Group not found"}
    
    verifier = group.get("settings", {}).get("verifier", {})
    verifier_token = verifier.get("botToken", "")
    
    if not verifier_token:
        return {"success": False, "error": "Verifier bot token not set"}
    
    webhook_url = f"https://own-zone.ru/tg-util/api/verifier-webhook/{group_id}"
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"https://api.telegram.org/bot{verifier_token}/setWebhook",
            json={"url": webhook_url, "allowed_updates": ["message"]}
        )
        return {"success": True, "result": response.json()}


# === Telegram profile update ===
async def update_telegram_profile(session_id: str, user_data: dict) -> dict:
    if not API_ID or not API_HASH:
        return {"success": False, "error": "API not configured"}
    
    session_path = SESSIONS_DIR / f"{session_id}.session"
    if not session_path.exists():
        return {"success": False, "error": "Session not found"}
    
    try:
        from telethon import TelegramClient
        from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest
        from telethon.tl.functions.photos import UploadProfilePhotoRequest
        from telethon.errors import UsernameOccupiedError, UsernameInvalidError, UsernameNotModifiedError
        
        client = TelegramClient(str(session_path.with_suffix('')), int(API_ID), API_HASH)
        await client.connect()
        
        if not await client.is_user_authorized():
            await client.disconnect()
            return {"success": False, "error": "Not authorized"}
        
        results = {}
        errors = []
        
        await asyncio.sleep(0.5)
        try:
            await client(UpdateProfileRequest(
                first_name=user_data.get("firstName", "") or "",
                last_name=user_data.get("lastName", "") or "",
                about=user_data.get("bio", "") or ""
            ))
            results["profile"] = "ok"
        except Exception as e:
            errors.append(f"Профиль: {e}")
        
        if user_data.get("username"):
            await asyncio.sleep(1)
            try:
                await client(UpdateUsernameRequest(username=user_data["username"]))
                results["username"] = "ok"
            except UsernameOccupiedError:
                errors.append(f"Username @{user_data['username']} уже занят")
            except UsernameInvalidError:
                errors.append(f"Username @{user_data['username']} недопустим")
            except UsernameNotModifiedError:
                pass
            except Exception as e:
                errors.append(f"Username: {e}")
        
        photo_path = PHOTOS_DIR / f"{user_data.get('id', '')}.jpg"
        if photo_path.exists():
            await asyncio.sleep(1)
            try:
                uploaded = await client.upload_file(str(photo_path))
                await client(UploadProfilePhotoRequest(file=uploaded))
                results["photo"] = "ok"
            except Exception as e:
                errors.append(f"Фото: {e}")
        
        await client.disconnect()
        
        return {"success": len(errors) == 0, "results": results, "errors": errors if errors else None}
        
    except Exception as e:
        return {"success": False, "error": str(e)}


# === API: Users ===
@app.get("/users")
async def get_users():
    return {"success": True, "data": load_users()}


@app.post("/users")
async def create_user(data: UserCreate):
    users = load_users()
    new_user = {
        "id": str(uuid.uuid4())[:8],
        "firstName": data.firstName,
        "lastName": data.lastName,
        "username": data.username,
        "bio": data.bio,
        "photoUrl": None,
        "sessionIds": [],
        "createdAt": datetime.now().isoformat()
    }
    users.append(new_user)
    save_users(users)
    return {"success": True, "data": new_user}


@app.patch("/users/{user_id}")
async def update_user(user_id: str, data: UserUpdate):
    users = load_users()
    user = next((u for u in users if u["id"] == user_id), None)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    for field in ["firstName", "lastName", "username", "bio"]:
        val = getattr(data, field)
        if val is not None:
            user[field] = val
    
    save_users(users)
    return {"success": True, "data": user}


@app.delete("/users/{user_id}")
async def delete_user(user_id: str):
    users = load_users()
    users = [u for u in users if u["id"] != user_id]
    save_users(users)
    
    sessions = load_sessions_meta()
    for s in sessions:
        if s["userId"] == user_id:
            s["userId"] = None
    save_sessions_meta(sessions)
    
    for photo in PHOTOS_DIR.glob(f"{user_id}.*"):
        photo.unlink()
    
    return {"success": True}


@app.post("/users/{user_id}/photo")
async def upload_user_photo(user_id: str, photo: UploadFile = File(...)):
    users = load_users()
    user = next((u for u in users if u["id"] == user_id), None)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    photo_path = PHOTOS_DIR / f"{user_id}.jpg"
    content = await photo.read()
    with open(photo_path, "wb") as f:
        f.write(content)
    
    photo_url = f"/tg-util/photos/{user_id}.jpg?t={int(datetime.now().timestamp())}"
    user["photoUrl"] = photo_url
    save_users(users)
    
    return {"success": True, "data": {"photoUrl": photo_url}}


@app.post("/users/{user_id}/apply")
async def apply_user_to_sessions(user_id: str):
    users = load_users()
    user = next((u for u in users if u["id"] == user_id), None)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    sessions = load_sessions_meta()
    assigned = [s for s in sessions if s["userId"] == user_id]
    
    results = []
    all_errors = []
    
    for i, session in enumerate(assigned):
        if i > 0:
            await asyncio.sleep(2)
        
        result = await update_telegram_profile(session["id"], user)
        results.append({"session": session["id"], "result": result})
        
        if result.get("errors"):
            all_errors.extend([f"{session['id']}: {e}" for e in result["errors"]])
    
    return {
        "success": len(all_errors) == 0,
        "data": {
            "applied": len(assigned),
            "results": results,
            "errors": all_errors if all_errors else None
        }
    }


# === API: Sessions ===
@app.get("/sessions")
async def get_sessions():
    sessions = sync_sessions()
    return {"success": True, "data": sessions}


@app.post("/sessions/upload")
async def upload_session(session: UploadFile = File(...)):
    if not session.filename or not session.filename.endswith(".session"):
        raise HTTPException(status_code=400, detail="Must be .session file")
    
    session_id = Path(session.filename).stem
    session_path = SESSIONS_DIR / f"{session_id}.session"
    
    if session_path.exists():
        return {"success": False, "error": f"Сессия '{session_id}' уже существует"}
    
    content = await session.read()
    with open(session_path, "wb") as f:
        f.write(content)
    
    session_data = {
        "id": session_id,
        "filename": session_path.name,
        "phone": "",
        "userId": None,
        "isAuthorized": True,
        "uploadedAt": datetime.now().isoformat()
    }
    
    sessions = load_sessions_meta()
    sessions.append(session_data)
    save_sessions_meta(sessions)
    
    return {"success": True, "data": session_data}


@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    session_path = SESSIONS_DIR / f"{session_id}.session"
    if session_path.exists():
        session_path.unlink()
    
    sessions = load_sessions_meta()
    sessions = [s for s in sessions if s["id"] != session_id]
    save_sessions_meta(sessions)
    
    return {"success": True}


@app.post("/sessions/{session_id}/assign")
async def assign_session(session_id: str, data: SessionAssign):
    sessions = load_sessions_meta()
    session = next((s for s in sessions if s["id"] == session_id), None)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session["userId"] = data.userId
    save_sessions_meta(sessions)
    
    return {"success": True, "data": session}


@app.post("/sessions/auto-assign")
async def auto_assign_sessions():
    users = load_users()
    sessions = load_sessions_meta()
    
    if not users:
        return {"success": False, "error": "No users"}
    
    unassigned = [s for s in sessions if not s["userId"]]
    
    if not unassigned:
        return {"success": True, "data": {"assigned": 0}}
    
    session_count = {u["id"]: 0 for u in users}
    for s in sessions:
        if s["userId"] and s["userId"] in session_count:
            session_count[s["userId"]] += 1
    
    users_without_sessions = [u for u in users if session_count[u["id"]] == 0]
    
    assigned_count = 0
    
    for i, user in enumerate(users_without_sessions):
        if i < len(unassigned):
            unassigned[i]["userId"] = user["id"]
            assigned_count += 1
    
    remaining = unassigned[assigned_count:]
    for session in remaining:
        min_user = min(users, key=lambda u: session_count[u["id"]])
        session["userId"] = min_user["id"]
        session_count[min_user["id"]] += 1
        assigned_count += 1
    
    save_sessions_meta(sessions)
    
    return {"success": True, "data": {"assigned": assigned_count}}


# === API: Groups ===
@app.get("/groups")
async def get_groups():
    groups = load_groups()
    for g in groups:
        g["isRunning"] = g["id"] in active_chats
        g["isCompleted"] = g["id"] in completed_groups
        g["status"] = chat_status.get(g["id"])
        g["bots"] = get_group_bots(g["id"])
    return {"success": True, "data": groups}


@app.get("/groups/{group_id}")
async def get_group(group_id: str):
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    group["isRunning"] = group_id in active_chats
    group["isCompleted"] = group_id in completed_groups
    group["status"] = chat_status.get(group_id)
    group["bots"] = get_group_bots(group_id)
    
    return {"success": True, "data": group}


@app.post("/groups/{group_id}/refresh")
async def refresh_group(group_id: str):
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    group["isRunning"] = group_id in active_chats
    group["isCompleted"] = group_id in completed_groups
    group["status"] = chat_status.get(group_id)
    group["bots"] = get_group_bots(group_id)
    
    return {"success": True, "data": group}


@app.get("/groups/{group_id}/bots")
async def get_group_bots_api(group_id: str):
    bots = get_group_bots(group_id)
    return {"success": True, "data": bots}


@app.post("/groups/{group_id}/assign-bot")
async def assign_bot(group_id: str, data: BotAssign):
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    if "settings" not in group:
        group["settings"] = GroupSettings().dict()
    
    if "botAssignments" not in group["settings"]:
        group["settings"]["botAssignments"] = {}
    
    if data.userId:
        group["settings"]["botAssignments"][data.botName] = data.userId
    else:
        group["settings"]["botAssignments"].pop(data.botName, None)
    
    save_groups(groups)
    
    group["isRunning"] = group_id in active_chats
    group["status"] = chat_status.get(group_id)
    group["bots"] = get_group_bots(group_id)
    
    return {"success": True, "data": group}


@app.post("/groups/{group_id}/auto-assign-bots")
async def auto_assign_bots(group_id: str):
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    users = load_users()
    bots = get_bots_from_text()
    
    if not users or not bots:
        return {"success": False, "error": "No users or bots"}
    
    if "settings" not in group:
        group["settings"] = GroupSettings().dict()
    
    group["settings"]["botAssignments"] = {}
    
    assigned_users = set()
    unassigned_bots = []
    
    for bot in bots:
        bot_name_lower = bot["name"].lower()
        bot_name_clean = re.sub(r'[\[\]()_\-\s.]', '', bot_name_lower)
        
        best_match = None
        best_score = 0
        
        for user in users:
            if user["id"] in assigned_users:
                continue
            
            username = (user.get("username") or "").lower()
            firstname = (user.get("firstName") or "").lower()
            username_clean = re.sub(r'[\[\]()_\-\s.]', '', username)
            firstname_clean = re.sub(r'[\[\]()_\-\s.]', '', firstname)
            
            score = 0
            if bot_name_clean == username_clean or bot_name_clean == firstname_clean:
                score = 100
            elif username_clean and (username_clean in bot_name_clean or bot_name_clean in username_clean):
                score = 80
            elif firstname_clean and (firstname_clean in bot_name_clean or bot_name_clean in firstname_clean):
                score = 70
            
            if score > best_score:
                best_score = score
                best_match = user
        
        if best_match and best_score >= 70:
            group["settings"]["botAssignments"][bot["name"]] = best_match["id"]
            assigned_users.add(best_match["id"])
        else:
            unassigned_bots.append(bot)
    
    remaining_users = [u for u in users if u["id"] not in assigned_users]
    for bot in unassigned_bots:
        if remaining_users:
            user = remaining_users.pop(0)
            group["settings"]["botAssignments"][bot["name"]] = user["id"]
    
    save_groups(groups)
    
    return {"success": True, "data": {"assigned": len(group["settings"]["botAssignments"])}}


@app.delete("/groups/{group_id}")
async def delete_group(group_id: str):
    if group_id in active_chats:
        active_chats.pop(group_id).cancel()
    if group_id in deletion_tasks:
        deletion_tasks.pop(group_id).cancel()
    if group_id in chat_status:
        del chat_status[group_id]
    
    groups = load_groups()
    groups = [g for g in groups if g["id"] != group_id]
    save_groups(groups)
    
    return {"success": True}


@app.patch("/groups/{group_id}/settings")
async def update_group_settings(group_id: str, settings: GroupSettings):
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    group["settings"] = settings.dict()
    save_groups(groups)
    
    # Setup verifier webhook if enabled
    verifier = settings.verifier
    if verifier.get("enabled") and verifier.get("botToken"):
        asyncio.create_task(setup_verifier_webhook(group_id))
    
    group["isRunning"] = group_id in active_chats
    group["status"] = chat_status.get(group_id)
    group["bots"] = get_group_bots(group_id)
    
    return {"success": True, "data": group}


@app.post("/groups/{group_id}/start")
async def start_chat(group_id: str):
    if group_id in active_chats:
        return {"success": False, "error": "Already running"}
    
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    completed_groups.discard(group_id)
    
    settings = GroupSettings(**group.get("settings", {}))
    
    # Auto-verify sessions if verifier enabled
    if settings.verifier.get("enabled"):
        await auto_verify_sessions(group_id, settings)
    
    task = asyncio.create_task(run_chat_imitation(group_id, settings))
    active_chats[group_id] = task
    
    return {"success": True}


@app.post("/groups/{group_id}/stop")
async def stop_chat(group_id: str):
    if group_id not in active_chats:
        return {"success": False, "error": "Not running"}
    
    active_chats.pop(group_id).cancel()
    
    if group_id in deletion_tasks:
        deletion_tasks.pop(group_id).cancel()
    
    if group_id in chat_status:
        del chat_status[group_id]
    
    return {"success": True}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "users": len(load_users()),
        "sessions": len(sync_sessions()),
        "groups": len(load_groups()),
        "active_chats": list(active_chats.keys())
    }


@app.get("/debug/{group_id}")
async def debug_group(group_id: str):
    """Debug endpoint to check group settings and webhook status"""
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    
    if not group:
        return {"error": "Group not found"}
    
    settings = group.get("settings", {})
    verifier = settings.get("verifier", {})
    
    # Check main bot webhook
    main_webhook_info = None
    if BOT_TOKEN:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getWebhookInfo")
                main_webhook_info = response.json()
        except Exception as e:
            main_webhook_info = {"error": str(e)}
    
    # Check verifier bot webhook
    verifier_webhook_info = None
    verifier_token = verifier.get("botToken", "")
    if verifier_token:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"https://api.telegram.org/bot{verifier_token}/getWebhookInfo")
                verifier_webhook_info = response.json()
        except Exception as e:
            verifier_webhook_info = {"error": str(e)}
    
    # Get verified users for this group
    verified_users = list(load_verified_users(group_id))
    
    return {
        "group_id": group_id,
        "group_title": group.get("title"),
        "settings": {
            "deleteSystemMessages": settings.get("deleteSystemMessages", False),
            "verifier": {
                "enabled": verifier.get("enabled", False),
                "botToken": "***" + verifier_token[-10:] if verifier_token else None,
                "botUsername": verifier.get("botUsername", ""),
                "messageText": verifier.get("messageText", ""),
                "buttonText": verifier.get("buttonText", "")
            }
        },
        "main_bot_token": "***" + BOT_TOKEN[-10:] if BOT_TOKEN else None,
        "main_webhook": main_webhook_info,
        "verifier_webhook": verifier_webhook_info,
        "verified_users": verified_users,
        "verified_users_count": len(verified_users)
    }


# === Chat Imitation ===
async def run_chat_imitation(group_id: str, settings: GroupSettings):
    logger.info(f"[{group_id}] Starting chat imitation")
    
    completed_groups.discard(group_id)
    
    text_file = TEXTS_DIR / "text_bad_1.txt"
    messages = parse_text_file(text_file)
    
    if not messages:
        logger.error(f"[{group_id}] No messages")
        return
    
    filtered = [m for m in messages if settings.lineRangeStart <= m["line"] <= settings.lineRangeEnd]
    if not filtered:
        logger.error(f"[{group_id}] No messages in range")
        return
    
    sessions = load_sessions_meta()
    bot_assignments = settings.botAssignments or {}
    
    user_sessions: Dict[str, List[dict]] = defaultdict(list)
    for s in sessions:
        if s["userId"]:
            user_sessions[s["userId"]].append(s)
    
    valid_bots = {}
    for bot_name, user_id in bot_assignments.items():
        if user_id and user_id in user_sessions and user_sessions[user_id]:
            valid_bots[bot_name] = {
                "userId": user_id,
                "sessions": user_sessions[user_id],
                "sessionIdx": 0
            }
    
    if not valid_bots:
        logger.error(f"[{group_id}] No valid bot assignments with sessions")
        return
    
    valid_messages = [m for m in filtered if m["name"] in valid_bots]
    if not valid_messages:
        logger.error(f"[{group_id}] No messages with valid bot assignments")
        return
    
    total_messages = len(valid_messages)
    logger.info(f"[{group_id}] Total valid messages: {total_messages}")
    
    joined_sessions: set = set()
    
    try:
        chat_id = int(group_id)
    except:
        logger.error(f"[{group_id}] Invalid group ID")
        return
    
    invite_hash = None
    if settings.inviteLink:
        match = re.search(r'(?:t\.me/\+|t\.me/joinchat/)([a-zA-Z0-9_-]+)', settings.inviteLink)
        if match:
            invite_hash = match.group(1)
    
    hour = datetime.now().hour
    is_night = hour >= settings.nightStartHour or hour < settings.nightEndHour
    avg_pause = (settings.nightPauseMin + settings.nightPauseMax) / 2 if is_night else (settings.dayPauseMin + settings.dayPauseMax) / 2
    
    try:
        from telethon import TelegramClient
        from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
        from telethon.tl.types import ChatInviteAlready
        from telethon.errors import UserAlreadyParticipantError, ChatWriteForbiddenError, ChannelPrivateError
        
        msg_idx = 0
        while msg_idx < len(valid_messages):
            msg = valid_messages[msg_idx]
            bot_name, text = msg["name"], msg["text"]
            line_num = msg["line"]
            
            remaining = len(valid_messages) - msg_idx
            estimated_minutes = int(remaining * avg_pause)
            
            chat_status[group_id] = {
                "isRunning": True,
                "currentLine": line_num,
                "currentMessage": text[:100],
                "currentBotName": bot_name,
                "totalMessages": total_messages,
                "remainingMessages": remaining,
                "estimatedMinutes": estimated_minutes
            }
            
            bot_info = valid_bots[bot_name]
            bot_sessions = bot_info["sessions"]
            session = bot_sessions[bot_info["sessionIdx"] % len(bot_sessions)]
            bot_info["sessionIdx"] += 1
            
            session_id = session["id"]
            session_path = SESSIONS_DIR / f"{session_id}.session"
            
            try:
                client = TelegramClient(str(session_path.with_suffix('')), int(API_ID), API_HASH)
                await client.connect()
                
                if not await client.is_user_authorized():
                    await client.disconnect()
                    msg_idx += 1
                    continue
                
                entity = None
                need_to_join = session_id not in joined_sessions
                
                if need_to_join and invite_hash:
                    try:
                        invite_info = await client(CheckChatInviteRequest(invite_hash))
                        
                        if isinstance(invite_info, ChatInviteAlready):
                            try:
                                entity = await client.get_entity(chat_id)
                                from telethon.tl.functions.channels import GetParticipantRequest
                                me = await client.get_me()
                                await client(GetParticipantRequest(entity, me))
                                joined_sessions.add(session_id)
                            except:
                                try:
                                    result = await client(ImportChatInviteRequest(invite_hash))
                                    entity = result.chats[0] if result.chats else None
                                    joined_sessions.add(session_id)
                                    await asyncio.sleep(30)
                                except:
                                    pass
                        else:
                            result = await client(ImportChatInviteRequest(invite_hash))
                            entity = result.chats[0] if result.chats else None
                            joined_sessions.add(session_id)
                            await asyncio.sleep(30)
                            
                    except UserAlreadyParticipantError:
                        joined_sessions.add(session_id)
                        entity = await client.get_entity(chat_id)
                    except Exception as e:
                        logger.error(f"[{group_id}] Join error: {e}")
                        await client.disconnect()
                        msg_idx += 1
                        continue
                
                if session_id in joined_sessions and entity is None:
                    try:
                        entity = await client.get_entity(chat_id)
                    except:
                        pass
                
                if entity:
                    try:
                        await client.send_message(entity, text)
                        logger.info(f"[{group_id}] #{line_num} [{msg_idx+1}/{total_messages}] {bot_name}: {text[:40]}...")
                    except (ChatWriteForbiddenError, ChannelPrivateError):
                        joined_sessions.discard(session_id)
                    except Exception as e:
                        logger.error(f"[{group_id}] Send error: {e}")
                
                await client.disconnect()
                
            except Exception as e:
                logger.error(f"[{group_id}] Session error: {e}")
            
            msg_idx += 1
            
            if msg_idx >= len(valid_messages):
                logger.info(f"[{group_id}] All messages sent! Completing...")
                completed_groups.add(group_id)
                if group_id in chat_status:
                    del chat_status[group_id]
                if group_id in active_chats:
                    del active_chats[group_id]
                return
            
            hour = datetime.now().hour
            is_night = hour >= settings.nightStartHour or hour < settings.nightEndHour
            pause = random.randint(
                settings.nightPauseMin if is_night else settings.dayPauseMin,
                settings.nightPauseMax if is_night else settings.dayPauseMax
            )
            
            if random.randint(1, 100) <= settings.randomPauseChance:
                pause += random.randint(settings.randomPauseMin, settings.randomPauseMax)
            
            logger.info(f"[{group_id}] Waiting {pause} min")
            await asyncio.sleep(pause * 60)
            
    except asyncio.CancelledError:
        logger.info(f"[{group_id}] Stopped")
        if group_id in chat_status:
            del chat_status[group_id]
    except Exception as e:
        logger.error(f"[{group_id}] Error: {e}", exc_info=True)
        if group_id in chat_status:
            del chat_status[group_id]
