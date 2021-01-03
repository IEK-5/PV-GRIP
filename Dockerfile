FROM osgeo/gdal:ubuntu-full-latest

RUN apt-get update -y
RUN apt-get install -y python3-pip libspatialindex-dev bc pdal rabbitmq-server git

ADD ./requirements.txt .
ADD ./README.md .
ADD ./setup.py .
RUN pip3 install -r requirements.txt
RUN pip3 install -e .

RUN mkdir /code
ADD . /code/
WORKDIR /code

CMD scripts/start_server.sh

EXPOSE 8080
