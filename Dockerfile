FROM osgeo/gdal:ubuntu-full-latest

RUN apt-get update -y
RUN apt-get install -y python3-pip libspatialindex-dev bc pdal

ADD ./requirements.txt .
RUN pip3 install -r requirements.txt

RUN mkdir /code
ADD . /code/
WORKDIR /code

CMD python3 server.py

EXPOSE 8080
