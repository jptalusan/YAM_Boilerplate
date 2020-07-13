FROM linusmotu/rsu-base:1.0

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ADD ./requirements.txt /usr/src/app/requirements.txt

RUN pip3 install -r requirements.txt

CMD ["python3", "-O", "/usr/src/app/worker.py"]