FROM mundialis/grass-py3-pdal:stable-ubuntu

RUN apt-get update -y
RUN apt-get install -y \
    python3-pip libspatialindex-dev \
    bc pdal \
    redis git \
    iproute2 \
    librsvg2-dev libtool

RUN mkdir /code
ADD . /code/
WORKDIR /code

RUN make init

CMD scripts/start.sh

EXPOSE 8080
EXPOSE 6379
