# bot.py
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
from django.db.models import F, Sum
from core.models import (
    KV, Daily, UserProfile,
    VoiceUserDaily, VoiceUserTotal,
    MessageUserDaily, MessageUserTotal,
    VoiceChannel, VoiceChannelDaily, VoiceUserChannelDaily,
)

# ====== ENV ======
GS_SHEET_ID = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID', '').strip()
GS_SA       = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON', '').strip()
GUILD_ID    = int(os.getenv('GUILD_ID', '0') or 0)


MAX_PIVOT_DATES = int(os.getenv('GS_MAX_PIVOT_DATES', '31'))

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
def kv_get_sync(key: str, default: str = '') -> str:
    obj, _ = KV.objects.get_or_create(key=key, defaults={'val': default})
    return obj.val

def kv_set_sync(key: str, val: str):
    KV.objects.update_or_create(key=key, defaults={'val': val})

def kv_get_total_sync():
    return int(kv_get_sync('messages_total', '0') or '0')

def kv_set_total_sync(v: int):
    kv_set_sync('messages_total', str(v))

def ensure_daily_sync(members: int | None = None):
    d = _today()
    row, _ = Daily.objects.get_or_create(
        date=d,
        defaults={'members': members or 0, 'messages_total': kv_get_total_sync()},
    )
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
    tot, _ = MessageUserTotal.objects.get_or_create(user_id=uid, defaults={'messages': 0})
    MessageUserTotal.objects.filter(pk=tot.pk).update(messages=F('messages') + by)
    d = _today()
    row, _ = MessageUserDaily.objects.get_or_create(date=d, user_id=uid, defaults={'messages': 0})
    MessageUserDaily.objects.filter(pk=row.pk).update(messages=F('messages') + by)

def flush_voice_sync(uid: str, sec: int, channel_id: str | None):
    if sec <= 0:
        return
    d = _today()

    Daily.objects.filter(date=d).update(voice_seconds=F('voice_seconds') + sec)

    row, _ = VoiceUserDaily.objects.get_or_create(date=d, user_id=uid, defaults={'seconds': 0})
    VoiceUserDaily.objects.filter(pk=row.pk).update(seconds=F('seconds') + sec)
    tot, _ = VoiceUserTotal.objects.get_or_create(user_id=uid, defaults={'seconds': 0})
    VoiceUserTotal.objects.filter(pk=tot.pk).update(seconds=F('seconds') + sec)

    if channel_id:
        ch, _ = VoiceChannel.objects.get_or_create(channel_id=str(channel_id), defaults={'name': '', 'is_stage': False})
        chrow, _ = VoiceChannelDaily.objects.get_or_create(date=d, channel_id=ch.channel_id, defaults={'seconds': 0})
        VoiceChannelDaily.objects.filter(pk=chrow.pk).update(seconds=F('seconds') + sec)

        urow, _ = VoiceUserChannelDaily.objects.get_or_create(
            date=d, channel_id=ch.channel_id, user_id=uid, defaults={'seconds': 0}
        )
        VoiceUserChannelDaily.objects.filter(pk=urow.pk).update(seconds=F('seconds') + sec)

# ===== async wrappers
ensure_daily     = sync_to_async(ensure_daily_sync, thread_sensitive=True)
inc_daily        = sync_to_async(inc_daily_sync, thread_sensitive=True)
upsert_profile   = sync_to_async(upsert_profile_sync, thread_sensitive=True)
upsert_channel   = sync_to_async(upsert_channel_sync, thread_sensitive=True)
inc_message_user = sync_to_async(inc_message_user_sync, thread_sensitive=True)
kv_get_total     = sync_to_async(kv_get_total_sync, thread_sensitive=True)
kv_set_total     = sync_to_async(kv_set_total_sync, thread_sensitive=True)
kv_get           = sync_to_async(kv_get_sync, thread_sensitive=True)
kv_set           = sync_to_async(kv_set_sync, thread_sensitive=True)
flush_voice      = sync_to_async(flush_voice_sync, thread_sensitive=True)

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
        voice_start[uid] = (_now(), ch_id)
    return max(sec, 0), ch_id

# ============= Google Sheets export (PIVOT) =============
def _gs_log(*args):
    print("[GSHEETS]", *args, flush=True)

def _col_idx_to_letter(idx: int) -> str:
    s = ""
    while idx > 0:
        idx, r = divmod(idx - 1, 26)
        s = chr(65 + r) + s
    return s

def _load_service_account():
    if not (GS_SHEET_ID and GS_SA):
        _gs_log("NO CREDS: GOOGLE_SHEETS_SPREADSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON is empty")
        return None, None
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if GS_SA.strip().startswith('{'):
        info = json.loads(GS_SA)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(GS_SA, scopes=scopes)
    gc = gspread.authorize(creds)
    return creds, gc

def _ws_upsert(sh, title):
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=1000, cols=26)

def _ensure_pivot_sheet_with_header(sh, title: str, base_header: list[str]):

    ws = _ws_upsert(sh, title)
    vals = ws.get_all_values() or []
    if not vals:
        ws.update(values=[base_header], range_name='A1')
    return ws

def _trim_old_date_columns(ws, base_cols_count: int):

    header = ws.row_values(1) or []
    if len(header) <= base_cols_count:
        return
    date_cols = header[base_cols_count:]
    if len(date_cols) <= MAX_PIVOT_DATES:
        return
   
    to_remove = len(date_cols) - MAX_PIVOT_DATES

    start = base_cols_count       
    end   = base_cols_count + to_remove  
    ws.spreadsheet.batch_update({
        "requests": [{
            "deleteDimension": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "COLUMNS",
                    "startIndex": start,
                    "endIndex": end
                }
            }
        }]
    })

def _pivot_incremental_sync(export_date: _dt.date):

    date_str = str(export_date)
    _gs_log(f"INCREMENTAL: start for {date_str}")
    creds, gc = _load_service_account()
    if not gc:
        _gs_log("INCREMENTAL: aborted — no creds")
        return
    sh = gc.open_by_key(GS_SHEET_ID)

    # ===== USERS PIVOT =====
    base_users_header = ['user_id', 'username', 'total_hours']
    ws = _ensure_pivot_sheet_with_header(sh, "VoiceUsersPivot", base_users_header)
    header = ws.row_values(1) or base_users_header

   
    if date_str not in header:
        ws.update(values=[header + [date_str]], range_name='1:1')
        _trim_old_date_columns(ws, base_cols_count=len(base_users_header))
        header = ws.row_values(1) or base_users_header

    col_total = 3 
    col_date  = header.index(date_str) + 1

    values = ws.get_all_values() or [header]
    uid_to_row = {}
    for i in range(2, len(values) + 1):
        row = values[i - 1]
        if row and len(row) >= 1 and row[0].strip():
            uid_to_row[row[0].strip()] = i

    last_row = max(uid_to_row.values()) if uid_to_row else 1
    if last_row >= 2:
        col_letter = _col_idx_to_letter(col_date)
        ws.spreadsheet.values_update(
            f"VoiceUsersPivot!{col_letter}2:{col_letter}{last_row}",
            params={"valueInputOption": "RAW"},
            body={"values": [[0.0]] * (last_row - 1)},
        )


    day_map = {
        str(r['user_id']): round((int(r['sec'] or 0))/3600, 2)
        for r in VoiceUserDaily.objects.filter(date=export_date).values('user_id').annotate(sec=Sum('seconds'))
    }

    totals = {
        str(r['user_id']): round((int(r['sec'] or 0))/3600, 2)
        for r in VoiceUserTotal.objects.values('user_id').annotate(sec=Sum('seconds'))
    }
    usernames = {str(p['user_id']): (p.get('username') or '')
                 for p in UserProfile.objects.values('user_id', 'username')}


    updates = []
    for uid, hours in day_map.items():
        if uid in uid_to_row:
            rnum = uid_to_row[uid]
            updates.append({"range": f"VoiceUsersPivot!{_col_idx_to_letter(col_date)}{rnum}",
                            "majorDimension": "ROWS", "values": [[hours]]})
            updates.append({"range": f"VoiceUsersPivot!{_col_idx_to_letter(col_total)}{rnum}",
                            "majorDimension": "ROWS", "values": [[totals.get(uid, 0.0)]]})
    if updates:
        ws.spreadsheet.values_batch_update(body={"valueInputOption": "RAW", "data": updates})


    width = len(header)
    new_rows = []
    for uid, hours in day_map.items():
        if uid not in uid_to_row:
            row = [''] * width
            row[0] = uid
            row[1] = usernames.get(uid, '')
            row[2] = totals.get(uid, 0.0)

            for c in range(4, width + 1):
                row[c - 1] = 0.0
            row[col_date - 1] = hours
            new_rows.append(row)
    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW", table_range="A1")

    _gs_log(f"INCREMENTAL: UsersPivot set {len(day_map)} values for {date_str} (+{len(new_rows)} new rows)")

    # ===== CHANNELS PIVOT =====
    base_ch_header = ['channel_id', 'channel_name', 'total_hours']
    ws = _ensure_pivot_sheet_with_header(sh, "VoiceChannelsPivot", base_ch_header)
    header = ws.row_values(1) or base_ch_header

    if date_str not in header:
        ws.update(values=[header + [date_str]], range_name='1:1')
        _trim_old_date_columns(ws, base_cols_count=len(base_ch_header))
        header = ws.row_values(1) or base_ch_header

    col_total = 3
    col_date  = header.index(date_str) + 1

    values = ws.get_all_values() or [header]
    cid_to_row = {}
    for i in range(2, len(values) + 1):
        row = values[i - 1]
        if row and len(row) >= 1 and row[0].strip():
            cid_to_row[row[0].strip()] = i

    last_row = max(cid_to_row.values()) if cid_to_row else 1
    if last_row >= 2:
        col_letter = _col_idx_to_letter(col_date)
        ws.spreadsheet.values_update(
            f"VoiceChannelsPivot!{col_letter}2:{col_letter}{last_row}",
            params={"valueInputOption": "RAW"},
            body={"values": [[0.0]] * (last_row - 1)},
        )

    day_map = {
        str(r['channel_id']): round((int(r['sec'] or 0))/3600, 2)
        for r in VoiceChannelDaily.objects.filter(date=export_date).values('channel_id').annotate(sec=Sum('seconds'))
    }
    totals = {
        str(r['channel_id']): round((int(r['sec'] or 0))/3600, 2)
        for r in VoiceChannelDaily.objects.values('channel_id').annotate(sec=Sum('seconds'))
    }
    names = {str(c['channel_id']): (c.get('name') or '')
             for c in VoiceChannel.objects.values('channel_id', 'name')}

    updates = []
    for cid, hours in day_map.items():
        if cid in cid_to_row:
            rnum = cid_to_row[cid]
            updates.append({"range": f"VoiceChannelsPivot!{_col_idx_to_letter(col_date)}{rnum}",
                            "majorDimension": "ROWS", "values": [[hours]]})
            updates.append({"range": f"VoiceChannelsPivot!{_col_idx_to_letter(col_total)}{rnum}",
                            "majorDimension": "ROWS", "values": [[totals.get(cid, 0.0)]]})
    if updates:
        ws.spreadsheet.values_batch_update(body={"valueInputOption": "RAW", "data": updates})

    width = len(header)
    new_rows = []
    for cid, hours in day_map.items():
        if cid not in cid_to_row:
            row = [''] * width
            row[0] = cid
            row[1] = names.get(cid, '')
            row[2] = totals.get(cid, 0.0)
            for c in range(4, width + 1):
                row[c - 1] = 0.0
            row[col_date - 1] = hours
            new_rows.append(row)
    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW", table_range="A1")

    _gs_log(f"INCREMENTAL: ChannelsPivot set {len(day_map)} values for {date_str} (+{len(new_rows)} new rows)")
    _gs_log("INCREMENTAL: done")

pivot_incremental = sync_to_async(_pivot_incremental_sync, thread_sensitive=True)


async def _settle_voice_until(cutoff_dt: _dt.datetime):
    cutoff_dt = cutoff_dt.astimezone(timezone.get_current_timezone())
    for uid, (start_dt, ch_id) in list(voice_start.items()):
        if ch_id and start_dt < cutoff_dt:
            sec = int((cutoff_dt - start_dt).total_seconds())
            if sec > 0:
                await flush_voice(uid, sec, ch_id)
                voice_start[uid] = (cutoff_dt, ch_id)


async def _daily_noon_export():
    while True:
        now_local = timezone.localtime()
        target = now_local.replace(hour=12, minute=0, second=0, microsecond=0)
        if now_local >= target:
            target = target + datetime.timedelta(days=1)
        delay = (target - now_local).total_seconds()
        await asyncio.sleep(delay)


        try:
            export_date = _today() - datetime.timedelta(days=1)
            await pivot_incremental(export_date)
        except Exception as e:
            print("[GSHEETS] noon export failed:", repr(e), flush=True)

# ============= Discord events =============
@client.event
async def on_ready():
    g = client.get_guild(GUILD_ID)
    await ensure_daily(g.member_count if g else 0)

   
    if g:
        for ch in g.channels:
            if str(getattr(ch, 'type', '')) in ('voice', 'stage_voice'):
                await upsert_channel(ch)

        for m in g.members:
            if getattr(m, "voice", None) and m.voice and m.voice.channel and not getattr(m, "bot", False):
                await upsert_channel(m.voice.channel)
                voice_start[str(m.id)] = (_now(), str(m.voice.channel.id))


    try:
        await pivot_incremental(_today() - datetime.timedelta(days=1))
    except Exception as e:
        print("[GSHEETS] initial incremental error:", repr(e), flush=True)

    
    asyncio.create_task(_voice_flusher(60))

    asyncio.create_task(_daily_noon_export())

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

    if not before.channel and after.channel:
        await upsert_channel(after.channel)
        voice_start[uid] = (_now(), str(after.channel.id))
        return

    if before.channel and not after.channel:
        sec, ch_id = _add_local_delta(uid)
        voice_start.pop(uid, None)
        if sec:
            await flush_voice(uid, sec, ch_id)
        return

    if before.channel and after.channel and before.channel.id != after.channel.id:
        sec, ch_id = _add_local_delta(uid)
        if sec:
            await flush_voice(uid, sec, ch_id)
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

# ============= run =============
if __name__ == "__main__" and not os.getenv("BOT_NO_RUN"):
    client.run(os.getenv('DISCORD_TOKEN'))