#! /bin/bash

python manage.py makemigrations --no-input

python manage.py migrate --no-input

python manage.py collectstatic --no-input

python manage.py runserver 0.0.0.0:8000

#python manage.py createsuperuser -u admin -e admin@example.com -pass admin --noinput
#
#python manage.py changepassword admin
