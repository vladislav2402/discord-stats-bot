from django.http import JsonResponse, HttpResponseBadRequest, HttpResponse
from django.db.models import Sum
from django.utils import timezone
from datetime import datetime

from .models import (
    KV, Daily, UserProfile,
    VoiceUserDaily, VoiceUserTotal,
    MessageUserDaily, MessageUserTotal
)


def _today():
    return timezone.localdate()

def _get_total_messages():
    try:
        return int(KV.objects.get(pk='messages_total').val)
    except KV.DoesNotExist:
        KV.objects.create(key='messages_total', val='0')
        return 0

def _profile_map(user_ids):
    rows = UserProfile.objects.filter(user_id__in=user_ids).values(
        'user_id', 'username', 'display_name', 'avatar_url', 'joined_at'
    )
    d = {}
    for r in rows:
        d[r['user_id']] = {
            'username': r['username'] or '',
            'display_name': r['display_name'] or '',
            'avatar_url': r['avatar_url'] or "https://cdn.discordapp.com/embed/avatars/0.png",
            'joined_at': r.get('joined_at')
        }
    return d

def _excel_safe(v):
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)

    try:
        return v.isoformat()
    except Exception:
        return v


def now(request):
    d = _today()
    row, _ = Daily.objects.get_or_create(
        date=d,
        defaults={
            'members': 0, 'joins': 0, 'leaves': 0,
            'messages': 0, 'messages_total': _get_total_messages(),
            'voice_seconds': 0,
        },
    )

    active_authors = MessageUserDaily.objects.filter(date=d).values('user_id').distinct().count()
    active_voice   = VoiceUserDaily.objects.filter(date=d).values('user_id').distinct().count()
    visitors       = max(active_authors, active_voice)
    avg_per_active = round(row.messages / active_authors, 2) if active_authors else 0.0

    return JsonResponse({
        'date': str(d),
        'members': row.members,
        'joins': row.joins,
        'leaves': row.leaves,
        'diff': row.joins - row.leaves,
        'messages_today': row.messages,
        'messages_total': row.messages_total,
        'voice_hours_today': round((row.voice_seconds or 0) / 3600, 2),
        'unique_message_members': active_authors,
        'visitors': visitors,
        'avg_messages_per_active_member': avg_per_active,
    })

def history(request):
    rows = list(
        Daily.objects.order_by('-date').values(
            'date', 'members', 'joins', 'leaves',
            'messages', 'messages_total', 'voice_seconds'
        )
    )
    for r in rows:
        r['voice_hours'] = round((r['voice_seconds'] or 0) / 3600, 2)
    return JsonResponse(rows, safe=False)


def voice_today(request):
    d = _today()
    rows = list(
        VoiceUserDaily.objects.filter(date=d)
        .order_by('-seconds')
        .values('user_id', 'seconds')
    )
    prof = _profile_map([r['user_id'] for r in rows])
    out = []
    for r in rows:
        p = prof.get(r['user_id'], {})
        sec = r['seconds'] or 0
        out.append({
            'user_id': r['user_id'],
            'username': p.get('username', ''),
            'display_name': p.get('display_name', ''),
            'avatar_url': p.get('avatar_url'),
            'seconds': int(sec),
            'hours': round(sec / 3600, 2),
        })
    return JsonResponse(out, safe=False)

def voice_by_date(request):
    q = request.GET.get('date')
    if not q:
        return HttpResponseBadRequest('date required YYYY-MM-DD')
    rows = list(
        VoiceUserDaily.objects.filter(date=q)
        .order_by('-seconds')
        .values('user_id', 'seconds')
    )
    prof = _profile_map([r['user_id'] for r in rows])
    out = []
    for r in rows:
        p = prof.get(r['user_id'], {})
        sec = r['seconds'] or 0
        out.append({
            'user_id': r['user_id'],
            'username': p.get('username', ''),
            'display_name': p.get('display_name', ''),
            'avatar_url': p.get('avatar_url'),
            'seconds': int(sec),
            'hours': round(sec / 3600, 2),
        })
    return JsonResponse(out, safe=False)


def voice_user_today(request, user_id):
    d = _today()
    sec = VoiceUserDaily.objects.filter(date=d, user_id=user_id)\
        .values_list('seconds', flat=True).first() or 0
    prof = _profile_map([user_id]).get(user_id, {'user_id': user_id})
    return JsonResponse({
        'user': prof,
        'seconds': int(sec),
        'hours': round(int(sec) / 3600, 2),
    })

def voice_user_history(request, user_id):
    rows = list(
        VoiceUserDaily.objects.filter(user_id=user_id)
        .order_by('-date')
        .values('date', 'seconds')
    )
    out = [{'date': str(r['date']),
            'seconds': int(r['seconds'] or 0),
            'hours': round((r['seconds'] or 0) / 3600, 2)} for r in rows]
    return JsonResponse(out, safe=False)

def voice_user_total(request, user_id):
    tot = VoiceUserTotal.objects.filter(user_id=user_id)\
        .values_list('seconds', flat=True).first() or 0
    return JsonResponse({
        'user_id': user_id,
        'seconds': int(tot),
        'hours': round(int(tot) / 3600, 2),
    })


def messages_users_today(request):
    d = _today()
    rows = list(
        MessageUserDaily.objects.filter(date=d)
        .order_by('-messages')
        .values('user_id', 'messages')
    )
    prof = _profile_map([r['user_id'] for r in rows])
    out = []
    for r in rows:
        p = prof.get(r['user_id'], {})
        out.append({
            'user_id': r['user_id'],
            'username': p.get('username', ''),
            'display_name': p.get('display_name', ''),
            'avatar_url': p.get('avatar_url'),
            'messages': int(r['messages'] or 0),
        })
    return JsonResponse(out, safe=False)

def messages_user_today(request, user_id):
    d = _today()
    cnt = MessageUserDaily.objects.filter(date=d, user_id=user_id)\
        .values_list('messages', flat=True).first() or 0
    prof = _profile_map([user_id]).get(user_id, {'user_id': user_id})
    return JsonResponse({'user': prof, 'messages': int(cnt)})

def messages_user_history(request, user_id):
    rows = MessageUserDaily.objects.filter(user_id=user_id)\
        .order_by('-date').values('date', 'messages')
    out = [{'date': str(r['date']), 'messages': int(r['messages'] or 0)} for r in rows]
    return JsonResponse(out, safe=False)

def messages_user_total(request, user_id):
    total = MessageUserTotal.objects.filter(user_id=user_id)\
        .values_list('messages', flat=True).first()
    if total is None:
        total = MessageUserDaily.objects.filter(user_id=user_id)\
            .aggregate(s=Sum('messages'))['s'] or 0
    return JsonResponse({'user_id': user_id, 'messages': int(total)})


def user_today(request, user_id):
    d = _today()
    prof = _profile_map([user_id]).get(user_id, {'user_id': user_id})
    voice_sec = VoiceUserDaily.objects.filter(date=d, user_id=user_id)\
        .values_list('seconds', flat=True).first() or 0
    msg_cnt = MessageUserDaily.objects.filter(date=d, user_id=user_id)\
        .values_list('messages', flat=True).first() or 0
    return JsonResponse({
        'user': prof,
        'voice_seconds': int(voice_sec),
        'voice_hours': round(int(voice_sec) / 3600, 2),
        'messages': int(msg_cnt),
    })


def export_xlsx(request):

    from openpyxl import Workbook
    import io

    wb = Workbook()

    ws = wb.active
    ws.title = 'Daily'
    ws.append(['date','members','joins','leaves','messages','messages_total','voice_seconds','voice_hours'])
    for r in Daily.objects.order_by('date').values(
        'date','members','joins','leaves','messages','messages_total','voice_seconds'
    ):
        ws.append([
            _excel_safe(r['date']),
            r['members'] or 0,
            r['joins'] or 0,
            r['leaves'] or 0,
            r['messages'] or 0,
            r['messages_total'] or 0,
            r['voice_seconds'] or 0,
            round((r['voice_seconds'] or 0)/3600, 2),
        ])


    ws2 = wb.create_sheet('VoiceByDay')
    ws2.append(['user_id','date','seconds','hours'])
    for r in VoiceUserDaily.objects.order_by('date','user_id').values('user_id','date','seconds'):
        sec = r['seconds'] or 0
        ws2.append([r['user_id'], _excel_safe(r['date']), sec, round(sec/3600, 2)])


    ws3 = wb.create_sheet('MessagesByDay')
    ws3.append(['user_id','date','messages'])
    for r in MessageUserDaily.objects.order_by('date','user_id').values('user_id','date','messages'):
        ws3.append([r['user_id'], _excel_safe(r['date']), r['messages'] or 0])


    ws4 = wb.create_sheet('Profiles')
    ws4.append(['user_id','username','display_name','avatar_url','joined_at','is_bot'])
    for p in UserProfile.objects.order_by('user_id').values('user_id','username','display_name','avatar_url','joined_at','is_bot'):
        ws4.append([
            p['user_id'],
            p.get('username') or '',
            p.get('display_name') or '',
            p.get('avatar_url') or '',
            _excel_safe(p.get('joined_at') or ''),
            bool(p.get('is_bot')),
        ])

    bio = io.BytesIO()
    wb.save(bio); bio.seek(0)
    resp = HttpResponse(
        bio.getvalue(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    resp['Content-Disposition'] = 'attachment; filename="discord_metrics_all.xlsx"'
    return resp


def export_xlsx_today(request):
    from openpyxl import Workbook
    import io

    d = _today()
    wb = Workbook()

    ws = wb.active
    ws.title = 'Daily'
    row = Daily.objects.filter(date=d).values(
        'date','members','joins','leaves','messages','messages_total','voice_seconds'
    ).first() or {}
    ws.append(['date','members','joins','leaves','messages','messages_total','voice_seconds','voice_hours'])
    ws.append([
        _excel_safe(d),
        row.get('members', 0),
        row.get('joins', 0),
        row.get('leaves', 0),
        row.get('messages', 0),
        row.get('messages_total', 0),
        row.get('voice_seconds', 0),
        round((row.get('voice_seconds', 0) or 0)/3600, 2),
    ])


    ws2 = wb.create_sheet('VoiceToday')
    ws2.append(['user_id','seconds','hours'])
    for r in VoiceUserDaily.objects.filter(date=d).values('user_id','seconds'):
        sec = r['seconds'] or 0
        ws2.append([r['user_id'], sec, round(sec/3600, 2)])


    ws3 = wb.create_sheet('MessagesToday')
    ws3.append(['user_id','messages'])
    for r in MessageUserDaily.objects.filter(date=d).values('user_id','messages'):
        ws3.append([r['user_id'], r['messages'] or 0])

    bio = io.BytesIO()
    wb.save(bio); bio.seek(0)
    resp = HttpResponse(
        bio.getvalue(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    resp['Content-Disposition'] = 'attachment; filename="discord_metrics_today.xlsx"'
    return resp
