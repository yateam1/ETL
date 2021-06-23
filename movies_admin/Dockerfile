FROM python:3.8.6

WORKDIR /usr/src/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN pip install --upgrade pip
COPY requirements/base.txt .
COPY requirements/dev.txt .
RUN pip install -r dev.txt
COPY . .