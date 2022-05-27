# syntax=docker/dockerfile:1
FROM python:3.8-slim-buster

WORKDIR /python-docker

COPY resources/requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

RUN celery -A app.celery worker --loglevel=info --detach
RUN celery beat --app app.celery --detach

EXPOSE 8000

CMD [ "python3", "app.py"]
