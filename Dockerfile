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
ADD ./pvgrip/storage/ipfs_io/requirements.txt /code/pvgrip/storage/ipfs_io/requirements.txt
WORKDIR /code/pvgrip/storage/ipfs_io
RUN pip3 install -r requirements.txt

FROM build-stage1 AS build-stage2
ADD ./pvgrip/storage/cassandra_io/requirements.txt /code/pvgrip/storage/cassandra_io/requirements.txt
WORKDIR /code/pvgrip/storage/cassandra_io
RUN pip3 install -r requirements.txt

FROM build-stage2 AS build-stage3
WORKDIR /code
ADD ./requirements.txt /code/requirements.txt
RUN pip3 install -r requirements.txt

FROM build-stage3 AS build-stage4
ADD ./scripts/install_ipfs.sh /code/scripts/install_ipfs.sh
RUN ./scripts/install_ipfs.sh

FROM build-stage4 AS build-stage5
ADD ./scripts/install_smrender.sh /code/scripts/install_smrender.sh
RUN ./scripts/install_smrender.sh

FROM build-stage5 AS build-stage6
ADD ./pvgrip/storage/cassandra_io /code/pvgrip/storage/cassandra_io
WORKDIR /code/pvgrip/storage/cassandra_io
RUN pip3 install -e .

FROM build-stage6 AS build-stage7
ADD ./pvgrip/storage/ipfs_io /code/pvgrip/storage/ipfs_io
WORKDIR /code/pvgrip/storage/ipfs_io
RUN pip3 install -e .

FROM build-stage7 AS build-stage8
WORKDIR /code
ADD ./pvgrip/ssdp/ssdp /code/pvgrip/ssdp/ssdp
ADD ./scripts/install_ssdp.sh /code/scripts/install_ssdp.sh
RUN ./scripts/install_ssdp.sh

FROM build-stage8 AS build-stage9
ADD . /code/
RUN pip3 install -e .
