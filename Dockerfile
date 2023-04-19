FROM python:3.9.16

ENV BASE_DIR /home

WORKDIR $BASE_DIR

RUN apt-get update -y

ADD ./src $BASE_DIR/src
ADD ./requirements.txt $BASE_DIR

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

WORKDIR $BASE_DIR/src

ENV PYTHONPATH=$PYTHONPATH:$BASE_DIR