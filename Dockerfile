FROM mundialis/grass-py3-pdal:stable-ubuntu

RUN apt-get update -y
RUN apt-get install -y \
    python3-pip libspatialindex-dev \
    bc pdal \
    redis git

RUN mkdir /code
ADD . /code/
WORKDIR /code

RUN make init

CMD scripts/start_server.sh

EXPOSE 8080
