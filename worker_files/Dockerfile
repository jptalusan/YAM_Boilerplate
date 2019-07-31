FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

RUN apt install -y git
RUN git clone https://github.com/linusmotu/osmnx.git
RUN git clone https://github.com/linusmotu/networkx.git

ADD ./requirements.txt /usr/src/app/requirements.txt

RUN pip3 install -r requirements.txt

WORKDIR /usr/src/app/osmnx
RUN pip3 install .

WORKDIR /usr/src/app/networkx
RUN pip3 install .

WORKDIR /usr/src/app

CMD ["python3", "-O", "/usr/src/app/worker.py"]