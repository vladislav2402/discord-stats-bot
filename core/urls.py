from django.urls import path
from . import views

urlpatterns = [
    path('now', views.now),
    path('history', views.history),

    path('voice/users/today', views.voice_today),
    path('voice/users/by-date', views.voice_by_date),

    path('voice/user/<str:user_id>/today', views.voice_user_today),
    path('voice/user/<str:user_id>/history', views.voice_user_history),
    path('voice/user/<str:user_id>/total', views.voice_user_total),

    path('messages/users/today', views.messages_users_today),
    path('messages/user/<str:user_id>/today', views.messages_user_today),
    path('messages/user/<str:user_id>/history', views.messages_user_history),
    path('messages/user/<str:user_id>/total', views.messages_user_total),

    path('user/<str:user_id>/today', views.user_today),

    path('export.xlsx', views.export_xlsx),         
    path('export/xlsx', views.export_xlsx),       
    path('export/today.xlsx', views.export_xlsx_today),
]
