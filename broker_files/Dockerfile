FROM python:3.6

# ZMQ REP Server
EXPOSE 7000 8000 9000

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ADD ./requirements.txt /usr/src/app/requirements.txt

RUN pip3 install -r requirements.txt

CMD ["python3", "-O", "/usr/src/app/broker.py"]
# CMD ["python3", "/usr/src/app/broker.py"]