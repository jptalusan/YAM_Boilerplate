FROM linusmotu/rsu-base:1.0

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ADD ./requirements.txt /usr/src/app/requirements.txt

RUN pip3 install -r requirements.txt

RUN pip3 uninstall networkx -y
RUN pip3 uninstall osmnx -y
RUN pip3 install osmnx

RUN rm -rf networkx
RUN git clone https://linusmotu:Dirknowitzki4!@github.com/linusmotu/networkx.git
WORKDIR /usr/src/app/networkx
RUN pip3 install .

WORKDIR /usr/src/app

CMD ["python3", "-O", "/usr/src/app/worker.py"]