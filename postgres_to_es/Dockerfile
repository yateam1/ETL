FROM python:3.9.5

WORKDIR /usr/src/postgres_to_es

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements/base.txt requirements/dev.txt requirements/production.txt ./
RUN pip install -r dev.txt

COPY . .

RUN chmod +x ./entrypoint.sh
ENTRYPOINT ["/usr/src/postgres_to_es/entrypoint.sh"]
