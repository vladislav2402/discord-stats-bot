from django.db import models


class KV(models.Model):
    key = models.CharField(primary_key=True, max_length=255)
    val = models.TextField(default='0')

    class Meta:
        db_table = 'core_kv'


class UserProfile(models.Model):
    user_id = models.CharField(primary_key=True, max_length=50)
    username = models.CharField(max_length=255, default='', blank=True)     
    display_name = models.CharField(max_length=255, default='', blank=True) 
    avatar_url = models.TextField(default='', blank=True)
    joined_at = models.DateTimeField(null=True, blank=True)
    left_at = models.DateTimeField(null=True, blank=True)
    is_bot = models.BooleanField(default=False)

    class Meta:
        db_table = 'core_userprofile'


class Daily(models.Model):
    
    id = models.BigAutoField(primary_key=True)
    date = models.DateField(db_index=True, unique=True)

    members = models.IntegerField(default=0)                
    joins = models.IntegerField(default=0)
    leaves = models.IntegerField(default=0)

    messages = models.IntegerField(default=0)              
    messages_total = models.BigIntegerField(default=0)   

    voice_seconds = models.BigIntegerField(default=0)     

    unique_message_members = models.IntegerField(default=0) 
    visitors = models.IntegerField(default=0)              
    avg_messages_per_active_member = models.FloatField(default=0.0)

    class Meta:
        db_table = 'core_daily'
        ordering = ['-date']


class VoiceUserDaily(models.Model):

    id = models.BigAutoField(primary_key=True)
    date = models.DateField(db_index=True)
    user = models.ForeignKey(UserProfile, on_delete=models.CASCADE, db_column='user_id', to_field='user_id')
    seconds = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'core_voiceuserdaily'
        indexes = [
            models.Index(fields=['date']),
            models.Index(fields=['user']),
        ]
        unique_together = [('date', 'user')]


class VoiceUserTotal(models.Model):

    user = models.OneToOneField(UserProfile, on_delete=models.CASCADE, primary_key=True, db_column='user_id', to_field='user_id')
    seconds = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'core_voiceusertotal'


class MessageUserDaily(models.Model):
    id = models.BigAutoField(primary_key=True)
    date = models.DateField(db_index=True)
    user = models.ForeignKey(UserProfile, on_delete=models.CASCADE, db_column='user_id', to_field='user_id')
    messages = models.IntegerField(default=0)

    class Meta:
        db_table = 'core_messageuserdaily'
        indexes = [
            models.Index(fields=['date']),
            models.Index(fields=['user']),
        ]
        unique_together = [('date', 'user')]


class MessageUserTotal(models.Model):

    user = models.OneToOneField(UserProfile, on_delete=models.CASCADE, primary_key=True, db_column='user_id', to_field='user_id')
    messages = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'core_messageusertotal'
