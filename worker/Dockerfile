ARG PYVERSION=3.12
ARG RELEASE=slim-bookworm

FROM python:$PYVERSION-$RELEASE

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN apt-get update \
    && apt-get install -y netcat-openbsd curl \
    && rm -rf /var/lib/apt/lists/*

RUN  pip install --upgrade pip\
     && pip install -r requirements.txt

COPY . .

CMD ["python", "processor.py"]