#!/bin/sh
set -e
if [ "$1" = "web" ]; then
  python manage.py makemigrations core --noinput
  python manage.py runserver 0.0.0.0:8000
elif [ "$1" = "bot" ]; then
  python bot.py
fi
