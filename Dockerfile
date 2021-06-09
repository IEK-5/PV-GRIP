FROM mundialis/grass-py3-pdal:stable-ubuntu AS build-stage0
RUN apt-get update -y && apt-get install -y \
    bc \
    git \
    iproute2 \
    libreadline-dev \
    librsvg2-dev \
    libspatialindex-dev \
    libtool \
    osmctools \
    pdal \
    python3-pip

EXPOSE 8080
EXPOSE 6379

RUN mkdir /code
WORKDIR /code

FROM build-stage0 AS build-stage1
ADD ./open_elevation/cassandra_io/requirements.txt /code/open_elevation/cassandra_io/requirements.txt
WORKDIR /code/open_elevation/cassandra_io
RUN pip3 install -r requirements.txt

FROM build-stage1 AS build-stage2
WORKDIR /code
ADD ./requirements.txt /code/requirements.txt
RUN pip3 install -r requirements.txt

FROM build-stage2 AS build-stage3
ADD ./scripts/install_smrender.sh /code/scripts/install_smrender.sh
RUN ./scripts/install_smrender.sh

FROM build-stage3 AS build-stage4
ADD ./open_elevation/cassandra_io /code/open_elevation/cassandra_io
WORKDIR /code/open_elevation/cassandra_io
RUN pip3 install -e .

FROM build-stage4 AS build-stage5
WORKDIR /code
ADD ./open_elevation/ssdp /code/open_elevation/ssdp
ADD ./scripts/install_ssdp.sh /code/scripts/install_ssdp.sh
RUN ./scripts/install_ssdp.sh

FROM build-stage5 AS build-stage6
ADD . /code/
RUN pip3 install -e .
