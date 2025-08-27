from django.db import models

class KV(models.Model):
    key = models.CharField(max_length=128, primary_key=True)
    val = models.TextField()

    class Meta:
        db_table = 'core_kv'


class Daily(models.Model):
    date = models.DateField(primary_key=True)
    members = models.IntegerField(default=0)
    joins = models.IntegerField(default=0)
    leaves = models.IntegerField(default=0)
    messages = models.IntegerField(default=0)
    messages_total = models.BigIntegerField(default=0)
    voice_seconds = models.BigIntegerField(default=0)

    unique_message_members = models.IntegerField(default=0)       # <= default=0
    avg_messages_per_active_member = models.FloatField(default=0) # <= default=0.0
    visitors = models.IntegerField(default=0)   

    class Meta:
        db_table = 'core_daily'
        ordering = ['-date']


class UserProfile(models.Model):
    user_id = models.CharField(max_length=32, primary_key=True)
    username = models.CharField(max_length=255, blank=True, default='')
    display_name = models.CharField(max_length=255, blank=True, default='')
    avatar_url = models.TextField(blank=True, default='')
    joined_at = models.DateTimeField(null=True, blank=True)
    is_bot = models.BooleanField(default=False)

    class Meta:
        db_table = 'core_userprofile'


# ------ Voice (per user aggregate & per day) ------
class VoiceUserTotal(models.Model):
    user = models.OneToOneField(UserProfile, to_field='user_id', on_delete=models.CASCADE, primary_key=True)
    seconds = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'core_voiceusertotal'


class VoiceUserDaily(models.Model):
    date = models.DateField()
    user = models.ForeignKey(UserProfile, to_field='user_id', on_delete=models.CASCADE)
    seconds = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'core_voiceuserdaily'
        unique_together = (('date', 'user'),)
        indexes = [models.Index(fields=['date']), models.Index(fields=['user'])]


# ------ Messages (per user aggregate & per day) ------
class MessageUserTotal(models.Model):
    user = models.OneToOneField(UserProfile, to_field='user_id', on_delete=models.CASCADE, primary_key=True)
    messages = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'core_messageusertotal'


class MessageUserDaily(models.Model):
    date = models.DateField()
    user = models.ForeignKey(UserProfile, to_field='user_id', on_delete=models.CASCADE)
    messages = models.IntegerField(default=0)

    class Meta:
        db_table = 'core_messageuserdaily'
        unique_together = (('date', 'user'),)
        indexes = [models.Index(fields=['date']), models.Index(fields=['user'])]


# ------ NEW: Voice Channels ------
class VoiceChannel(models.Model):
    channel_id = models.CharField(max_length=32, primary_key=True)
    name = models.CharField(max_length=255, blank=True, default='')
    is_stage = models.BooleanField(default=False)

    class Meta:
        db_table = 'core_voicechannel'


class VoiceChannelDaily(models.Model):
    date = models.DateField()
    channel = models.ForeignKey(VoiceChannel, to_field='channel_id', on_delete=models.CASCADE)
    seconds = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'core_voicechanneldaily'
        unique_together = (('date', 'channel'),)
        indexes = [models.Index(fields=['date']), models.Index(fields=['channel'])]


class VoiceUserChannelDaily(models.Model):
    date = models.DateField()
    channel = models.ForeignKey(VoiceChannel, to_field='channel_id', on_delete=models.CASCADE)
    user = models.ForeignKey(UserProfile, to_field='user_id', on_delete=models.CASCADE)
    seconds = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'core_voiceuserchanneldaily'
        unique_together = (('date', 'channel', 'user'),)
        indexes = [
            models.Index(fields=['date']),
            models.Index(fields=['channel']),
            models.Index(fields=['user']),
        ]