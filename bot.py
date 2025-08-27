import os
import json
import asyncio
import datetime
import discord
from django.utils import timezone

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')
import django
django.setup()

import datetime as _dt
from decimal import Decimal

from asgiref.sync import sync_to_async
from django.db.models import F, Count
from core.models import (
    KV, Daily, UserProfile,
    VoiceUserDaily, VoiceUserTotal,
    MessageUserDaily, MessageUserTotal,
    VoiceChannel, VoiceChannelDaily, VoiceUserChannelDaily,
)

# ====== ENV for Google Sheets ======
GS_SHEET_ID = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID', '').strip()  # ID таблицы
GS_SA = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON', '').strip()  # ПУТЬ к файлу или JSON-строка

GUILD_ID = int(os.getenv('GUILD_ID', '0') or 0)

intents = discord.Intents.none()
intents.guilds = True
intents.members = True
intents.messages = True
intents.message_content = True
intents.voice_states = True

client = discord.Client(intents=intents)

# ============= time helpers =============
def _today():
    return timezone.localdate()

def _now():
    return timezone.now()

# ============= sync DB helpers =============

def kv_get_total_sync():
    obj, _ = KV.objects.get_or_create(key='messages_total', defaults={'val': '0'})
    return int(obj.val)

def kv_set_total_sync(v: int):
    KV.objects.update_or_create(key='messages_total', defaults={'val': str(v)})

def ensure_daily_sync(members: int | None = None):
    d = _today()
    row, _ = Daily.objects.get_or_create(
        date=d,
        defaults={'members': members or 0, 'messages_total': kv_get_total_sync()},
    )
    # Только members обновляем по снапу/ready
    if members is not None and row.members != members:
        Daily.objects.filter(pk=row.pk).update(members=members)

def inc_daily_sync(field: str, by: int = 1):
    d = _today()
    ensure_daily_sync()
    Daily.objects.filter(date=d).update(**{field: F(field) + by})

def upsert_profile_sync(member: discord.Member):
    avatar_url = ''
    try:
        if getattr(member, 'avatar', None):
            avatar_url = member.avatar.url
        elif getattr(member, 'display_avatar', None):
            avatar_url = member.display_avatar.url
    except Exception:
        pass

    obj, _ = UserProfile.objects.update_or_create(
        user_id=str(member.id),
        defaults=dict(
            username=getattr(member, 'name', '') or '',
            display_name=getattr(member, 'display_name', '') or '',
            avatar_url=avatar_url or "https://cdn.discordapp.com/embed/avatars/0.png",
            joined_at=getattr(member, 'joined_at', None),
            is_bot=bool(getattr(member, 'bot', False)),
        ),
    )
    VoiceUserTotal.objects.get_or_create(user=obj, defaults={'seconds': 0})
    MessageUserTotal.objects.get_or_create(user=obj, defaults={'messages': 0})

def upsert_channel_sync(ch: discord.abc.GuildChannel | None):
    """Создаёт/обновляет VoiceChannel запись."""
    if not ch:
        return
    t = str(getattr(ch, 'type', ''))
    is_stage = (t == 'stage_voice')
    if t not in ('voice', 'stage_voice'):
        return
    VoiceChannel.objects.update_or_create(
        channel_id=str(ch.id),
        defaults={'name': getattr(ch, 'name', '') or '', 'is_stage': is_stage}
    )

def inc_message_user_sync(uid: str, by: int = 1):
    # тотал
    tot, _ = MessageUserTotal.objects.get_or_create(user_id=uid, defaults={'messages': 0})
    MessageUserTotal.objects.filter(pk=tot.pk).update(messages=F('messages') + by)
    # по дню
    d = _today()
    row, _ = MessageUserDaily.objects.get_or_create(date=d, user_id=uid, defaults={'messages': 0})
    MessageUserDaily.objects.filter(pk=row.pk).update(messages=F('messages') + by)

def flush_voice_sync(uid: str, sec: int, channel_id: str | None):
    """Инкремент всех голосовых счётчиков. channel_id обязателен для каналов."""
    if sec <= 0:
        return
    d = _today()

    # Общая дневная метрика:
    Daily.objects.filter(date=d).update(voice_seconds=F('voice_seconds') + sec)

    # По пользователю (день/тотал):
    row, _ = VoiceUserDaily.objects.get_or_create(date=d, user_id=uid, defaults={'seconds': 0})
    VoiceUserDaily.objects.filter(pk=row.pk).update(seconds=F('seconds') + sec)
    tot, _ = VoiceUserTotal.objects.get_or_create(user_id=uid, defaults={'seconds': 0})
    VoiceUserTotal.objects.filter(pk=tot.pk).update(seconds=F('seconds') + sec)

    # По каналу (день) и по пользователю в канале (день):
    if channel_id:
        ch, _ = VoiceChannel.objects.get_or_create(channel_id=str(channel_id), defaults={'name': '', 'is_stage': False})
        chrow, _ = VoiceChannelDaily.objects.get_or_create(date=d, channel_id=ch.channel_id, defaults={'seconds': 0})
        VoiceChannelDaily.objects.filter(pk=chrow.pk).update(seconds=F('seconds') + sec)

        urow, _ = VoiceUserChannelDaily.objects.get_or_create(
            date=d, channel_id=ch.channel_id, user_id=uid, defaults={'seconds': 0}
        )
        VoiceUserChannelDaily.objects.filter(pk=urow.pk).update(seconds=F('seconds') + sec)

# ===== async wrappers
ensure_daily = sync_to_async(ensure_daily_sync, thread_sensitive=True)
inc_daily = sync_to_async(inc_daily_sync, thread_sensitive=True)
upsert_profile = sync_to_async(upsert_profile_sync, thread_sensitive=True)
upsert_channel = sync_to_async(upsert_channel_sync, thread_sensitive=True)
inc_message_user = sync_to_async(inc_message_user_sync, thread_sensitive=True)
kv_get_total = sync_to_async(kv_get_total_sync, thread_sensitive=True)
kv_set_total = sync_to_async(kv_set_total_sync, thread_sensitive=True)
flush_voice = sync_to_async(flush_voice_sync, thread_sensitive=True)

# ============= runtime state =============
# uid -> (start_dt, channel_id)
voice_start: dict[str, tuple[datetime.datetime, str | None]] = {}

def _add_local_delta(uid: str) -> tuple[int, str | None]:
    t = voice_start.get(uid)
    if not t:
        return 0, None
    start_dt, ch_id = t
    sec = int((_now() - start_dt).total_seconds())
    if sec > 0:
        # «продлеваем» отсчёт
        voice_start[uid] = (_now(), ch_id)
    return max(sec, 0), ch_id

# ============= Google Sheets export =============

# === Upsert helpers (put near your Sheets helpers) ===
def _read_header(ws) -> list[str]:
    # Empty sheet -> no header yet
    try:
        header = ws.row_values(1)
    except Exception:
        header = []
    return header or []

def _ensure_header(ws, header_row: list[str]):
    current = _read_header(ws)
    if not current:
        ws.append_row(header_row, value_input_option="RAW")
    elif [h.strip() for h in current] != [h.strip() for h in header_row]:
        # Optional: keep as-is, or enforce the header (uncomment to enforce)
        # ws.update('1:1', [header_row])
        pass

def _get_all_values(ws) -> list[list[str]]:
    try:
        return ws.get_all_values()  # includes header row
    except Exception:
        return []

def _build_index_by_key(ws, header: list[str], key_cols: list[str]) -> dict[tuple, list[int]]:
    """
    Returns {key_tuple: [row_numbers...]} using 1-based sheet row numbers.
    Can hold duplicates; we’ll update the first and delete the rest.
    """
    values = _get_all_values(ws)
    if not values:
        return {}

    col_idx = { _norm(name): i for i, name in enumerate(header) }
    # allow slight header drift; match by normalized names
    missing = [k for k in key_cols if _norm(k) not in col_idx]
    if missing:
        return {}

    idx: dict[tuple, list[int]] = {}
    for r in range(2, len(values) + 1):  # skip header
        row = values[r - 1]
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        key = tuple(_norm(row[col_idx[_norm(k)]]) for k in key_cols)
        if all(key):
            idx.setdefault(key, []).append(r)
    return idx

def _row_to_key(row: list, header: list[str], key_cols: list[str]) -> tuple:
    name_to_val = { _norm(header[i]): (row[i] if i < len(header) else "") for i in range(len(header)) }
    return tuple(_norm(name_to_val.get(_norm(k), "")) for k in key_cols)

def _update_rows(ws, start_row_and_rows: list[tuple[int, list]]):
    """Batch update each row starting at its row number, in the correct worksheet."""
    if not start_row_and_rows:
        return

    data = []
    for rownum, row_values in start_row_and_rows:
        data.append({
            "range": _a1_in_sheet(ws, f"A{rownum}"),  # <-- include sheet name!
            "majorDimension": "ROWS",
            "values": [row_values],
        })

    # Always call the Spreadsheet-level batch update
    ws.spreadsheet.values_batch_update(body={
        "valueInputOption": "RAW",
        "data": data,
    })

def _append_rows(ws, rows_2d: list[list]):
    """Append rows at the end (after last non-empty)."""
    if not rows_2d:
        return
    CHUNK = 500
    for i in range(0, len(rows_2d), CHUNK):
        ws.append_rows(rows_2d[i:i+CHUNK], value_input_option="RAW", table_range="A1")

def _upsert(ws, header: list[str], key_cols: list[str], rows_2d: list[list]):
    _ensure_header(ws, header)
    index = _build_index_by_key(ws, header, key_cols)

    updates: list[tuple[int, list]] = []
    appends: list[list] = []
    to_delete: list[int] = []

    for row in rows_2d:
        # pad/truncate to header len
        if len(row) < len(header): row = row + [""] * (len(header) - len(row))
        elif len(row) > len(header): row = row[:len(header)]

        # IMPORTANT: normalize *key values* format before matching
        # format all key components as strings the same way you write them
        key = _row_to_key(row, header, key_cols)

        rows_for_key = index.get(key, [])
        if rows_for_key:
            # Replace first occurrence; delete others (true “REPLACE” semantics)
            updates.append((rows_for_key[0], row))
            if len(rows_for_key) > 1:
                to_delete.extend(rows_for_key[1:])
        else:
            appends.append(row)

    if updates:
        _update_rows(ws, updates)
    if appends:
        _append_rows(ws, appends)
    if to_delete:
        _delete_rows(ws, to_delete)


def _load_service_account():
    """
    Возвращает (creds, gspread_client) с учётом того, что
    GOOGLE_SERVICE_ACCOUNT_JSON может быть путём к файлу или JSON-строкой.
    """
    if not (GS_SHEET_ID and GS_SA):
        return None, None
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

    # Если переменная похожа на JSON — парсим, иначе считаем, что это путь к файлу
    if GS_SA.strip().startswith('{'):
        info = json.loads(GS_SA)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(GS_SA, scopes=scopes)

    gc = gspread.authorize(creds)
    return creds, gc

def _ws_upsert(sh, title):
    """Get worksheet by title, create if missing. Do NOT clear."""
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=1000, cols=26)

def _sheet_is_empty(ws) -> bool:
    # fast check: if A1 is empty, assume the sheet is empty
    try:
        return (ws.acell("A1").value or "").strip() == ""
    except Exception:
        # fallback (very small sheets): treat as empty on error
        return True

def _ensure_header(ws, header_row: list[str]):
    if _sheet_is_empty(ws):
        ws.append_row(header_row, value_input_option="RAW")

def _append_rows(ws, rows_2d):
    """Append many rows at once to the next free row."""
    if not rows_2d:
        return
    # gspread will append after the last non-empty row
    ws.append_rows(rows_2d, value_input_option="RAW", table_range="A1")

def _excel_safe(v):
    if v is None:
        return ""
    if isinstance(v, (_dt.datetime, _dt.date)):
        # ISO string so it’s serializable; Sheets will see text unless you format later
        return v.isoformat()
    if isinstance(v, Decimal):
        # or str(v) if you prefer exact textual representation
        return float(v)
    if isinstance(v, bool):
        return bool(v)
        # ints/floats/str are already fine
    return v

# --- add near your helpers ---
async def _settle_voice_until(cutoff_dt: _dt.datetime):
    """
    Flush partial voice durations for users who are still in voice up to cutoff_dt,
    then move their start pointer to cutoff_dt (so the new day starts clean).
    """
    # Make cutoff tz-aware like your stored timestamps
    cutoff_dt = cutoff_dt.astimezone(timezone.get_current_timezone())
    for uid, (start_dt, ch_id) in list(voice_start.items()):
        if ch_id and start_dt < cutoff_dt:
            sec = int((cutoff_dt - start_dt).total_seconds())
            if sec > 0:
                await flush_voice(uid, sec, ch_id)
                voice_start[uid] = (cutoff_dt, ch_id)

def _a1_in_sheet(ws, a1_range: str) -> str:
    # Quote the sheet title safely for A1 notation
    title = ws.title.replace("'", "''")
    return f"'{title}'!{a1_range}"

async def _daily_eod_export():
    """
    Every local midnight (+ small delay), settle voice up to midnight,
    upsert Daily/Voice/Msgs/Profiles for the PREVIOUS day.
    """
    while True:
        now_local = timezone.localtime()
        # sleep until next midnight + 5 seconds (gives a tiny buffer)
        next_midnight = (now_local + datetime.timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        delay = (next_midnight - now_local).total_seconds() + 5
        await asyncio.sleep(delay)

        # 1) settle voice up to exactly midnight
        try:
            # next_midnight in *aware* datetime
            cutoff = timezone.make_aware(next_midnight.replace(tzinfo=None), timezone.get_current_timezone())
            await _settle_voice_until(cutoff)
        except Exception as e:
            print("[GSHEETS] Voice settle failed:", repr(e), flush=True)

        # 2) refresh members count for the new day (optional)
        try:
            g = client.get_guild(GUILD_ID)
            await ensure_daily(g.member_count if g else None)
        except Exception as e:
            print("[GSHEETS] ensure_daily failed:", repr(e), flush=True)

        # 3) export the PREVIOUS day
        try:
            export_date = _today() - datetime.timedelta(days=1)
            await export_to_gsheets(export_date)
            print(f"[GSHEETS] EOD export done for {export_date}", flush=True)
        except Exception as e:
            print("[GSHEETS] EOD export failed:", repr(e), flush=True)

def _norm(s):  # normalize for comparisons
    return (str(s) if s is not None else "").strip().lower()

def _gs_log(*args):
    print("[GSHEETS]", *args, flush=True)

def _delete_rows(ws, row_numbers: list[int]):
    """
    Delete given 1-based row numbers. Must delete from bottom up so indices don't shift.
    """
    if not row_numbers:
        return
    requests = []
    for r in sorted(set(row_numbers), reverse=True):
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "ROWS",
                    "startIndex": r - 1,  # inclusive, 0-based
                    "endIndex": r,        # exclusive
                }
            }
        })
    ws.spreadsheet.batch_update({"requests": requests})


def _export_to_gsheets_sync(export_date):
    """
    Экспортирует СНИМОК за конкретную дату:
      - Daily (+ unique_message_members, avg_messages_per_active_member)
      - VoiceByChannelDay
      - VoiceUserByChannelDay
      - MessagesByDay
      - Profiles (текущий снимок)
    """
    creds, gc = _load_service_account()
    if not gc:
        _gs_log("Skipping: no GS creds/ID")
        return

    sh = gc.open_by_key(GS_SHEET_ID)
    _gs_log("Opened spreadsheet:", sh.title, "date:", export_date)

    # ---------------- Daily (key: date) ----------------
    ws = _ws_upsert(sh, "Daily")
    _gs_log("Upserting -> tab: Daily")
    daily_header = [
        'date', 'members', 'joins', 'leaves', 'messages', 'messages_total',
        'voice_seconds', 'voice_hours', 'unique_message_members', 'avg_messages_per_active_member'
    ]

    row = Daily.objects.filter(date=export_date).values(
        'members', 'joins', 'leaves', 'messages', 'messages_total', 'voice_seconds'
    ).first() or {}

    authors = MessageUserDaily.objects.filter(date=export_date).values('user_id').distinct().count()
    msgs = int(row.get('messages', 0) or 0)
    avg = round(msgs / authors, 2) if authors else 0.0

    daily_rows = [[
        str(export_date),                              # <-- stringify key
        row.get('members', 0), row.get('joins', 0), row.get('leaves', 0),
        msgs, row.get('messages_total', 0),
        row.get('voice_seconds', 0), round((row.get('voice_seconds', 0) or 0) / 3600, 2),
        authors, avg
    ]]
    _upsert(ws, daily_header, key_cols=['date'], rows_2d=daily_rows)

    # ---------------- VoiceByChannelDay (key: date, channel_id) ----------------
    ws2 = _ws_upsert(sh, "VoiceByChannelDay")
    _gs_log("Upserting -> tab: VoiceByChannelDay")
    vcd_header = ['date', 'channel_id', 'channel_name', 'seconds', 'hours']

    id2name = dict(VoiceChannel.objects.values_list('channel_id', 'name'))
    rows = []
    for r in VoiceChannelDaily.objects.filter(date=export_date).values('channel_id', 'seconds'):
        sec = int(r['seconds'] or 0)
        rows.append([
            str(export_date),                           # <-- stringify key
            str(r['channel_id']),                       # <-- stringify key
            id2name.get(r['channel_id'], ''),
            sec,
            round(sec / 3600, 2),
        ])
    _upsert(ws2, vcd_header, key_cols=['date', 'channel_id'], rows_2d=rows)

    # ---------------- VoiceUserByChannelDay (key: date, channel_id, user_id) ----------------
    ws3 = _ws_upsert(sh, "VoiceUserByChannelDay")
    _gs_log("Upserting -> tab: VoiceUserByChannelDay")
    vucd_header = ['date', 'channel_id', 'channel_name', 'user_id', 'seconds', 'hours']

    id2name = dict(VoiceChannel.objects.values_list('channel_id', 'name'))
    rows = []
    for r in VoiceUserChannelDaily.objects.filter(date=export_date).values('channel_id', 'user_id', 'seconds'):
        sec = int(r['seconds'] or 0)
        rows.append([
            str(export_date),                           # <-- stringify key
            str(r['channel_id']),                       # <-- stringify key
            id2name.get(r['channel_id'], ''),
            str(r['user_id']),                          # <-- stringify key
            sec,
            round(sec / 3600, 2),
        ])
    _upsert(ws3, vucd_header, key_cols=['date', 'channel_id', 'user_id'], rows_2d=rows)

    # ---------------- MessagesByDay (key: user_id, date) ----------------
    ws4 = _ws_upsert(sh, "MessagesByDay")
    _gs_log("Upserting -> tab: MessagesByDay")
    mbd_header = ['user_id', 'date', 'messages']

    rows = []
    for r in MessageUserDaily.objects.filter(date=export_date).values('user_id', 'messages'):
        rows.append([
            str(r['user_id']),                          # <-- stringify key
            str(export_date),                           # <-- stringify key
            int(r['messages'] or 0),
        ])
    _upsert(ws4, mbd_header, key_cols=['user_id', 'date'], rows_2d=rows)

    # ---------------- Profiles (key: user_id, username) ----------------
    ws5 = _ws_upsert(sh, "Profiles")
    _gs_log("Upserting -> tab: Profiles")
    prof_header = ['user_id', 'username', 'display_name', 'avatar_url', 'joined_at', 'is_bot']

    rows = []
    for p in UserProfile.objects.order_by('user_id').values(
            'user_id', 'username', 'display_name', 'avatar_url', 'joined_at', 'is_bot'):
        rows.append([
            _excel_safe(str(p['user_id'])),             # <-- stringify key
            _excel_safe(str(p.get('username') or '')),  # <-- stringify key
            _excel_safe(p.get('display_name') or ''),
            _excel_safe(p.get('avatar_url') or ''),
            _excel_safe(p.get('joined_at')),
            _excel_safe(p.get('is_bot')),
        ])
    _upsert(ws5, prof_header, key_cols=['user_id', 'username'], rows_2d=rows)


export_to_gsheets = sync_to_async(_export_to_gsheets_sync, thread_sensitive=True)

# ============= Discord events =============

@client.event
async def on_ready():
    g = client.get_guild(GUILD_ID)
    await ensure_daily(g.member_count if g else 0)

    # инвентаризация голосовых каналов
    if g:
        for ch in g.channels:
            if str(getattr(ch, 'type', '')) in ('voice', 'stage_voice'):
                await upsert_channel(ch)

        for m in g.members:
            if not m.bot:
                await upsert_profile(m)
                if m.voice and m.voice.channel:
                    await upsert_channel(m.voice.channel)
                    voice_start[str(m.id)] = (_now(), str(m.voice.channel.id))

    # мгновенная первичная заливка в Google Sheets (снимок на сейчас)
    asyncio.create_task(export_to_gsheets(_today()))

    asyncio.create_task(_voice_flusher(60))        # раз в минуту флашим
    asyncio.create_task(_daily_eod_export())          # в полночь: snapshot members + экспорт прошедшего дня

@client.event
async def on_message(msg):
    if not msg.guild or msg.guild.id != GUILD_ID or msg.author.bot:
        return
    await upsert_profile(msg.author)

    await inc_daily('messages', 1)
    total = await kv_get_total()
    await kv_set_total(total + 1)
    await inc_message_user(str(msg.author.id), 1)

@client.event
async def on_member_join(member):
    if member.guild.id != GUILD_ID or member.bot:
        return
    await upsert_profile(member)
    await inc_daily('joins', 1)
    g = client.get_guild(GUILD_ID)
    await ensure_daily(g.member_count if g else None)

@client.event
async def on_member_remove(member):
    if member.guild.id != GUILD_ID or member.bot:
        return
    await inc_daily('leaves', 1)
    g = client.get_guild(GUILD_ID)
    await ensure_daily(g.member_count if g else None)

@client.event
async def on_member_update(before, after):
    if after.guild.id != GUILD_ID or after.bot:
        return
    await upsert_profile(after)

@client.event
async def on_user_update(before, after):
    for g in client.guilds:
        m = g.get_member(after.id)
        if m and g.id == GUILD_ID and not m.bot:
            await upsert_profile(m)

@client.event
async def on_voice_state_update(member, before, after):
    if member.guild.id != GUILD_ID or member.bot:
        return
    await upsert_profile(member)
    uid = str(member.id)

    # вошёл в канал
    if not before.channel and after.channel:
        await upsert_channel(after.channel)
        voice_start[uid] = (_now(), str(after.channel.id))
        return

    # вышел совсем
    if before.channel and not after.channel:
        sec, ch_id = _add_local_delta(uid)
        voice_start.pop(uid, None)
        if sec:
            await flush_voice(uid, sec, ch_id)
        return

    # переход между каналами
    if before.channel and after.channel and before.channel.id != after.channel.id:
        # закрыть прошлый канал
        sec, ch_id = _add_local_delta(uid)
        if sec:
            await flush_voice(uid, sec, ch_id)
        # открыть новый
        await upsert_channel(after.channel)
        voice_start[uid] = (_now(), str(after.channel.id))

# ============= background tasks =============

async def _voice_flusher(period: int = 60):
    while True:
        await asyncio.sleep(period)
        for uid in list(voice_start.keys()):
            sec, ch_id = _add_local_delta(uid)
            if sec:
                await flush_voice(uid, sec, ch_id)

async def _midnight_tick():
    while True:
        n = timezone.localtime()
        nxt = (n + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((nxt - n).total_seconds())

        # обновим members на новый день
        g = client.get_guild(GUILD_ID)
        await ensure_daily(g.member_count if g else None)

        # экспорт прошедшего дня в Google Sheets (закрываем день)
        export_date = _today() - datetime.timedelta(days=1)
        try:
            await export_to_gsheets(export_date)
        except Exception as e:
            print("Google Sheets export error:", e)

# ============= run =============
client.run(os.getenv('DISCORD_TOKEN'))