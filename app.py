"""
TG Session Manager - Backend Server
Users + Sessions + Bots Architecture
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

VERSION_APP = "1.2"

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

# Active chats and their status
active_chats: Dict[str, asyncio.Task] = {}
chat_status: Dict[str, dict] = {}  # group_id -> {currentLine, currentMessage, currentBotName}

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
    botAssignments: Dict[str, str] = {}  # botName -> userId


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
    """Parse text file and extract bot names and messages"""
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
                messages.append({
                    "line": i,
                    "name": match.group(1).strip(),
                    "text": match.group(2)
                })
    return messages


def get_bots_from_text() -> List[dict]:
    """Get unique bots from text file with message counts"""
    text_file = TEXTS_DIR / "text_bad_1.txt"
    messages = parse_text_file(text_file)
    
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
    """Get bots for a specific group with assignments"""
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    
    bots = get_bots_from_text()
    
    if group and "settings" in group:
        assignments = group["settings"].get("botAssignments", {})
        for bot in bots:
            bot["userId"] = assignments.get(bot["name"])
    
    return bots


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
            results["profile"] = str(e)
            errors.append(f"Профиль: {e}")
        
        if user_data.get("username"):
            await asyncio.sleep(1)
            try:
                await client(UpdateUsernameRequest(username=user_data["username"]))
                results["username"] = "ok"
            except UsernameOccupiedError:
                results["username"] = "occupied"
                errors.append(f"Username @{user_data['username']} уже занят")
            except UsernameInvalidError:
                results["username"] = "invalid"
                errors.append(f"Username @{user_data['username']} недопустим")
            except UsernameNotModifiedError:
                results["username"] = "same"
            except Exception as e:
                results["username"] = str(e)
                errors.append(f"Username: {e}")
        
        photo_path = PHOTOS_DIR / f"{user_data.get('id', '')}.jpg"
        if photo_path.exists():
            await asyncio.sleep(1)
            try:
                uploaded = await client.upload_file(str(photo_path))
                await client(UploadProfilePhotoRequest(file=uploaded))
                results["photo"] = "ok"
            except Exception as e:
                results["photo"] = str(e)
                errors.append(f"Фото: {e}")
        
        await client.disconnect()
        logger.info(f"[{session_id}] Results: {results}")
        
        return {
            "success": len(errors) == 0,
            "results": results,
            "errors": errors if errors else None
        }
        
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
    """Apply user data to all assigned sessions with delays"""
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
    """Auto-assign unassigned sessions to users, prioritizing users without sessions"""
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
            session_count[user["id"]] += 1
            assigned_count += 1
    
    remaining = unassigned[assigned_count:]
    if remaining:
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
    group["status"] = chat_status.get(group_id)
    group["bots"] = get_group_bots(group_id)
    
    return {"success": True, "data": group}


@app.post("/groups/{group_id}/refresh")
async def refresh_group(group_id: str):
    """Refresh group info and bots"""
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    group["isRunning"] = group_id in active_chats
    group["status"] = chat_status.get(group_id)
    group["bots"] = get_group_bots(group_id)
    
    return {"success": True, "data": group}


@app.get("/groups/{group_id}/bots")
async def get_group_bots_api(group_id: str):
    bots = get_group_bots(group_id)
    return {"success": True, "data": bots}


@app.post("/groups/{group_id}/assign-bot")
async def assign_bot(group_id: str, data: BotAssign):
    """Assign a user to a bot"""
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
    """Auto-assign bots to users by matching usernames"""
    groups = load_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    users = load_users()
    bots = get_bots_from_text()
    
    if "settings" not in group:
        group["settings"] = GroupSettings().dict()
    
    if "botAssignments" not in group["settings"]:
        group["settings"]["botAssignments"] = {}
    
    assigned_count = 0
    
    for bot in bots:
        bot_name_lower = bot["name"].lower()
        
        for user in users:
            user_username = (user.get("username") or "").lower()
            user_first_name = (user.get("firstName") or "").lower()
            
            if (user_username and user_username in bot_name_lower) or \
               (user_first_name and user_first_name in bot_name_lower) or \
               (user_username and bot_name_lower in user_username) or \
               (user_first_name and bot_name_lower in user_first_name):
                group["settings"]["botAssignments"][bot["name"]] = user["id"]
                assigned_count += 1
                break
    
    save_groups(groups)
    
    return {"success": True, "data": {"assigned": assigned_count}}


@app.delete("/groups/{group_id}")
async def delete_group(group_id: str):
    if group_id in active_chats:
        active_chats.pop(group_id).cancel()
    
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
    
    settings = GroupSettings(**group.get("settings", {}))
    task = asyncio.create_task(run_chat_imitation(group_id, settings))
    active_chats[group_id] = task
    
    return {"success": True}


@app.post("/groups/{group_id}/stop")
async def stop_chat(group_id: str):
    if group_id not in active_chats:
        return {"success": False, "error": "Not running"}
    
    active_chats.pop(group_id).cancel()
    
    if group_id in chat_status:
        del chat_status[group_id]
    
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
            logger.info(f"[{group_id}] Using invite hash: {invite_hash}")
    
    if not invite_hash:
        logger.warning(f"[{group_id}] No invite link set")
    
    try:
        from telethon import TelegramClient
        from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
        from telethon.tl.types import ChatInviteAlready
        from telethon.errors import UserAlreadyParticipantError, ChatWriteForbiddenError, ChannelPrivateError
        
        msg_idx = 0
        while True:
            if msg_idx >= len(filtered):
                msg_idx = 0
            
            msg = filtered[msg_idx]
            bot_name, text = msg["name"], msg["text"]
            line_num = msg["line"]
            
            chat_status[group_id] = {
                "isRunning": True,
                "currentLine": line_num,
                "currentMessage": text[:100],
                "currentBotName": bot_name
            }
            
            if bot_name not in valid_bots:
                logger.warning(f"[{group_id}] Bot '{bot_name}' not assigned, skipping")
                msg_idx += 1
                continue
            
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
                    logger.warning(f"[{group_id}] Session {session_id} not authorized")
                    await client.disconnect()
                    msg_idx += 1
                    continue
                
                entity = None
                need_to_join = session_id not in joined_sessions
                
                if need_to_join and invite_hash:
                    try:
                        logger.info(f"[{group_id}] Session {session_id} checking invite...")
                        invite_info = await client(CheckChatInviteRequest(invite_hash))
                        
                        if isinstance(invite_info, ChatInviteAlready):
                            logger.info(f"[{group_id}] Session {session_id} already in group")
                            entity = invite_info.chat
                            joined_sessions.add(session_id)
                        else:
                            logger.info(f"[{group_id}] Session {session_id} joining via invite...")
                            result = await client(ImportChatInviteRequest(invite_hash))
                            entity = result.chats[0] if result.chats else None
                            joined_sessions.add(session_id)
                            logger.info(f"[{group_id}] Session {session_id} joined! Waiting 30s...")
                            await asyncio.sleep(30)
                            
                    except UserAlreadyParticipantError:
                        logger.info(f"[{group_id}] Session {session_id} already participant")
                        joined_sessions.add(session_id)
                        entity = await client.get_entity(chat_id)
                    except Exception as e:
                        logger.error(f"[{group_id}] Session {session_id} join error: {e}")
                        await client.disconnect()
                        msg_idx += 1
                        await asyncio.sleep(60)
                        continue
                
                if session_id in joined_sessions and entity is None:
                    try:
                        entity = await client.get_entity(chat_id)
                    except Exception as e:
                        logger.error(f"[{group_id}] Session {session_id} get_entity error: {e}")
                
                if entity:
                    try:
                        await client.send_message(entity, text)
                        logger.info(f"[{group_id}] #{line_num} {bot_name} ({session_id}): {text[:40]}...")
                    except ChatWriteForbiddenError:
                        logger.error(f"[{group_id}] Session {session_id} cannot write")
                        joined_sessions.discard(session_id)
                    except ChannelPrivateError:
                        logger.error(f"[{group_id}] Session {session_id} - channel is private")
                        joined_sessions.discard(session_id)
                    except Exception as e:
                        logger.error(f"[{group_id}] Session {session_id} send error: {e}")
                else:
                    logger.warning(f"[{group_id}] Session {session_id} no entity")
                
                await client.disconnect()
                
            except Exception as e:
                logger.error(f"[{group_id}] Session {session_id} error: {e}")
            
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
        if group_id in chat_status:
            del chat_status[group_id]
    except Exception as e:
        logger.error(f"[{group_id}] Error: {e}", exc_info=True)
        if group_id in chat_status:
            del chat_status[group_id]
