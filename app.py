"""
TG Session Manager - Backend Server
Users + Sessions Architecture
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

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from pydantic import BaseModel
from dotenv import load_dotenv
import logging

VERSION_APP = "1.0"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

SESSIONS_DIR = Path(os.getenv("TG_SESSIONS_DIR", "/opt/imitation_chat/sessions"))
CACHE_DIR = Path(os.getenv("TG_CACHE_DIR", "/opt/imitation_chat/cache"))
PHOTOS_DIR = CACHE_DIR / "photos"
TEXTS_DIR = Path(os.getenv("TG_TEXTS_DIR", "/opt/imitation_chat"))

USERS_FILE = CACHE_DIR / "users.json"
SESSIONS_FILE = CACHE_DIR / "sessions.json"
GROUPS_FILE = CACHE_DIR / "groups.json"

SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
PHOTOS_DIR.mkdir(parents=True, exist_ok=True)

active_chats: Dict[str, asyncio.Task] = {}

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


# === Data helpers ===
def load_json(path: Path) -> list:
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return []


def save_json(path: Path, data: list):
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
    """Sync session files with metadata"""
    meta = load_sessions_meta()
    meta_ids = {m["id"] for m in meta}
    
    # Add new session files
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
    
    # Remove deleted session files
    existing_files = {f.stem for f in SESSIONS_DIR.glob("*.session")}
    meta = [m for m in meta if m["id"] in existing_files]
    
    save_sessions_meta(meta)
    return meta


# === Telegram helpers ===
async def update_telegram_profile(session_id: str, user_data: dict) -> dict:
    """Update Telegram profile for a session"""
    logger.info(f"[{session_id}] Updating Telegram profile...")
    
    if not API_ID or not API_HASH:
        return {"success": False, "error": "API not configured"}
    
    session_path = SESSIONS_DIR / f"{session_id}.session"
    if not session_path.exists():
        return {"success": False, "error": "Session not found"}
    
    try:
        from telethon import TelegramClient
        from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest
        from telethon.tl.functions.photos import UploadProfilePhotoRequest
        
        client = TelegramClient(str(session_path.with_suffix('')), int(API_ID), API_HASH)
        await client.connect()
        
        if not await client.is_user_authorized():
            await client.disconnect()
            return {"success": False, "error": "Not authorized"}
        
        results = {}
        
        # Update profile
        try:
            await client(UpdateProfileRequest(
                first_name=user_data.get("firstName", "") or "",
                last_name=user_data.get("lastName", "") or "",
                about=user_data.get("bio", "") or ""
            ))
            results["profile"] = "ok"
        except Exception as e:
            results["profile"] = str(e)
        
        # Update username
        if user_data.get("username"):
            try:
                await client(UpdateUsernameRequest(username=user_data["username"]))
                results["username"] = "ok"
            except Exception as e:
                results["username"] = str(e)
        
        # Upload photo if exists
        photo_path = PHOTOS_DIR / f"{user_data.get('id', '')}.jpg"
        if photo_path.exists():
            try:
                uploaded = await client.upload_file(str(photo_path))
                await client(UploadProfilePhotoRequest(file=uploaded))
                results["photo"] = "ok"
            except Exception as e:
                results["photo"] = str(e)
        
        await client.disconnect()
        logger.info(f"[{session_id}] Results: {results}")
        return {"success": True, "results": results}
        
    except Exception as e:
        logger.error(f"[{session_id}] Error: {e}")
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
    
    # Unassign sessions
    sessions = load_sessions_meta()
    for s in sessions:
        if s["userId"] == user_id:
            s["userId"] = None
    save_sessions_meta(sessions)
    
    # Delete photo
    for photo in PHOTOS_DIR.glob(f"{user_id}.*"):
        photo.unlink()
    
    return {"success": True}


@app.post("/users/{user_id}/photo")
async def upload_user_photo(user_id: str, photo: UploadFile = File(...)):
    users = load_users()
    user = next((u for u in users if u["id"] == user_id), None)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Save photo
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
    """Apply user data to all assigned sessions"""
    users = load_users()
    user = next((u for u in users if u["id"] == user_id), None)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    sessions = load_sessions_meta()
    assigned = [s for s in sessions if s["userId"] == user_id]
    
    results = []
    for session in assigned:
        result = await update_telegram_profile(session["id"], user)
        results.append({"session": session["id"], "result": result})
    
    return {"success": True, "data": {"applied": len(assigned), "results": results}}


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
    
    # Check if exists
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
    """Auto-assign unassigned sessions to users"""
    users = load_users()
    sessions = load_sessions_meta()
    
    if not users:
        return {"success": False, "error": "No users"}
    
    unassigned = [s for s in sessions if not s["userId"]]
    
    for i, session in enumerate(unassigned):
        user = users[i % len(users)]
        session["userId"] = user["id"]
    
    save_sessions_meta(sessions)
    
    return {"success": True, "data": {"assigned": len(unassigned)}}


# === API: Groups ===
@app.get("/groups")
async def get_groups():
    groups = load_groups()
    for g in groups:
        g["isRunning"] = g["id"] in active_chats
    return {"success": True, "data": groups}


@app.delete("/groups/{group_id}")
async def delete_group(group_id: str):
    if group_id in active_chats:
        active_chats.pop(group_id).cancel()
    
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
    
    group["isRunning"] = group_id in active_chats
    return {"success": True, "data": group}


@app.post("/groups/{group_id}/start")
async def start_chat(group_id: str):
    if group_id in active_chats:
        return {"success": False, "error": "Already running"}
    
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    settings = GroupSettings(**group.get("settings", {}))
    task = asyncio.create_task(run_chat_imitation(group_id, settings))
    active_chats[group_id] = task
    
    return {"success": True}


@app.post("/groups/{group_id}/stop")
async def stop_chat(group_id: str):
    if group_id not in active_chats:
        return {"success": False, "error": "Not running"}
    
    active_chats.pop(group_id).cancel()
    return {"success": True}


# === Bot Webhook ===
@app.post("/webhook")
async def bot_webhook(request: Request):
    try:
        data = await request.json()
        logger.info(f"Webhook: {json.dumps(data, ensure_ascii=False)[:300]}")
        
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
            json={"url": webhook_url, "allowed_updates": ["my_chat_member", "message"]}
        )
        return {"success": True, "result": response.json()}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "users": len(load_users()),
        "sessions": len(sync_sessions()),
        "groups": len(load_groups()),
        "active_chats": list(active_chats.keys())
    }


# === Chat Imitation ===
def parse_text_file(filepath: Path) -> List[dict]:
    messages = []
    if not filepath.exists():
        return messages
    with open(filepath, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            match = re.match(r'^([^:]+):<(.+)>$', line)
            if match:
                messages.append({"line": i, "name": match.group(1).strip(), "text": match.group(2)})
    return messages


async def run_chat_imitation(group_id: str, settings: GroupSettings):
    logger.info(f"[{group_id}] Starting chat imitation")
    
    text_file = TEXTS_DIR / "text_bad_1.txt"
    messages = parse_text_file(text_file)
    
    if not messages:
        logger.error(f"[{group_id}] No messages")
        return
    
    filtered = [m for m in messages if settings.lineRangeStart <= m["line"] <= settings.lineRangeEnd]
    if not filtered:
        logger.error(f"[{group_id}] No messages in range")
        return
    
    # Get sessions with users
    sessions = load_sessions_meta()
    users = load_users()
    
    assigned_sessions = [s for s in sessions if s["userId"]]
    if not assigned_sessions:
        logger.error(f"[{group_id}] No assigned sessions")
        return
    
    # Map names to sessions
    name_to_session: Dict[str, dict] = {}
    session_idx = 0
    joined_sessions: set = set()
    
    try:
        chat_id = int(group_id)
    except:
        logger.error(f"[{group_id}] Invalid group ID")
        return
    
    # Get invite hash
    invite_hash = None
    if settings.inviteLink:
        match = re.search(r'(?:t\.me/\+|t\.me/joinchat/)([a-zA-Z0-9_-]+)', settings.inviteLink)
        if match:
            invite_hash = match.group(1)
    
    try:
        from telethon import TelegramClient
        from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
        from telethon.tl.types import ChatInviteAlready
        
        msg_idx = 0
        while True:
            if msg_idx >= len(filtered):
                msg_idx = 0
            
            msg = filtered[msg_idx]
            name, text = msg["name"], msg["text"]
            
            if name not in name_to_session:
                name_to_session[name] = assigned_sessions[session_idx % len(assigned_sessions)]
                session_idx += 1
            
            session = name_to_session[name]
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
                
                if session_id not in joined_sessions:
                    try:
                        entity = await client.get_entity(chat_id)
                        joined_sessions.add(session_id)
                    except:
                        if invite_hash:
                            try:
                                info = await client(CheckChatInviteRequest(invite_hash))
                                if isinstance(info, ChatInviteAlready):
                                    entity = info.chat
                                else:
                                    result = await client(ImportChatInviteRequest(invite_hash))
                                    entity = result.chats[0] if result.chats else None
                                    logger.info(f"[{group_id}] Session {session_id} joined, waiting 30s...")
                                    await asyncio.sleep(30)
                                joined_sessions.add(session_id)
                            except Exception as e:
                                logger.error(f"[{group_id}] Join error: {e}")
                else:
                    entity = await client.get_entity(chat_id)
                
                if entity:
                    await client.send_message(entity, text)
                    logger.info(f"[{group_id}] {name}: {text[:40]}...")
                
                await client.disconnect()
            except Exception as e:
                logger.error(f"[{group_id}] Send error: {e}")
            
            msg_idx += 1
            
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
    except Exception as e:
        logger.error(f"[{group_id}] Error: {e}", exc_info=True)
