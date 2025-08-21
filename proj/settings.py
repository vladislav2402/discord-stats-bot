import os
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

SECRET_KEY = os.getenv('DJANGO_SECRET_KEY','dev')
DEBUG = True
ALLOWED_HOSTS = ['*']

INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'django.contrib.staticfiles',
    'corsheaders',       
    'core',
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
]

CORS_ALLOW_ALL_ORIGINS = True  
ROOT_URLCONF = 'proj.urls'
WSGI_APPLICATION = 'proj.wsgi.application'

DATABASES = {
    'default': {
        'ENGINE':'django.db.backends.postgresql',
        'NAME': os.getenv('DB_NAME'),
        'USER': os.getenv('DB_USER'),
        'PASSWORD': os.getenv('DB_PASSWORD'),
        'HOST': os.getenv('DB_HOST'),
        'PORT': os.getenv('DB_PORT','5432'),
    }
}

TIME_ZONE = os.getenv('TZ','UTC')
USE_TZ = True

STATIC_URL = 'static/'
