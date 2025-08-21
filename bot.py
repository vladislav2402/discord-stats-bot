import os, asyncio, datetime, discord
from django.utils import timezone

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')
import django
django.setup()

from asgiref.sync import sync_to_async
from django.db.models import F
from core.models import (
    KV, Daily, UserProfile,
    VoiceUserDaily, VoiceUserTotal,
    MessageUserDaily, MessageUserTotal
)

GUILD_ID = int(os.getenv('GUILD_ID'))

intents = discord.Intents.none()
intents.guilds = True
intents.members = True
intents.messages = True
intents.message_content = True
intents.voice_states = True

client = discord.Client(intents=intents)


def _today():
    return timezone.localdate()

def _now():
    return timezone.now()


def kv_get_total_sync():
    obj, _ = KV.objects.get_or_create(key='messages_total', defaults={'val': '0'})
    return int(obj.val)

def kv_set_total_sync(v):
    KV.objects.update_or_create(key='messages_total', defaults={'val': str(v)})

def ensure_daily_sync(members=None):
    d = _today()
    row, _ = Daily.objects.get_or_create(
        date=d,
        defaults={'members': members or 0, 'messages_total': kv_get_total_sync()},
    )
    if members is not None and row.members != members:
        Daily.objects.filter(pk=row.pk).update(members=members)

def inc_daily_sync(field, by=1):
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

def inc_message_user_sync(uid: str, by=1):
    MessageUserTotal.objects.filter(user_id=uid).update(messages=F('messages') + by)
    d = _today()
    row, _ = MessageUserDaily.objects.get_or_create(date=d, user_id=uid, defaults={'messages': 0})
    MessageUserDaily.objects.filter(pk=row.pk).update(messages=F('messages') + by)

def flush_voice_sync(uid: str, sec: int):
    if sec <= 0:
        return
    d = _today()
    Daily.objects.filter(date=d).update(voice_seconds=F('voice_seconds') + sec)
    row, _ = VoiceUserDaily.objects.get_or_create(date=d, user_id=uid, defaults={'seconds': 0})
    VoiceUserDaily.objects.filter(pk=row.pk).update(seconds=F('seconds') + sec)
    VoiceUserTotal.objects.filter(user_id=uid).update(seconds=F('seconds') + sec)


ensure_daily = sync_to_async(ensure_daily_sync, thread_sensitive=True)
inc_daily = sync_to_async(inc_daily_sync, thread_sensitive=True)
upsert_profile = sync_to_async(upsert_profile_sync, thread_sensitive=True)
inc_message_user = sync_to_async(inc_message_user_sync, thread_sensitive=True)
kv_get_total = sync_to_async(kv_get_total_sync, thread_sensitive=True)
kv_set_total = sync_to_async(kv_set_total_sync, thread_sensitive=True)
flush_voice = sync_to_async(flush_voice_sync, thread_sensitive=True)

voice_start = {} 

def _add_local_delta(uid: str):
    t = voice_start.get(uid)
    if not t:
        return 0
    sec = int((_now() - t).total_seconds())
    if sec > 0:
        voice_start[uid] = _now()
    return max(sec, 0)


@client.event
async def on_ready():
    g = client.get_guild(GUILD_ID)
    await ensure_daily(g.member_count if g else 0)

    if g:
        for m in g.members:
            if not m.bot:
                await upsert_profile(m)
                if m.voice and m.voice.channel:
                    voice_start[str(m.id)] = _now()

    asyncio.create_task(_voice_flusher(60))
    asyncio.create_task(_midnight_members_snap())

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
        voice_start[uid] = _now()
        return

    if before.channel and not after.channel:
        sec = _add_local_delta(uid)
        voice_start.pop(uid, None)
        if sec:
            await flush_voice(uid, sec)
        return

    if before.channel and after.channel and before.channel.id != after.channel.id:
        sec = _add_local_delta(uid)
        if sec:
            await flush_voice(uid, sec)


async def _voice_flusher(period=60):
    while True:
        await asyncio.sleep(period)
        for uid in list(voice_start.keys()):
            sec = _add_local_delta(uid)
            if sec:
                await flush_voice(uid, sec)

async def _midnight_members_snap():
    while True:
        n = timezone.now()
        nxt = (n + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((nxt - n).total_seconds())
        g = client.get_guild(GUILD_ID)
        await ensure_daily(g.member_count if g else None)

client.run(os.getenv('DISCORD_TOKEN'))
