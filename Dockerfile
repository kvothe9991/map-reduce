FROM python:3.9.7-alpine

RUN pip install Pyro4

WORKDIR /

ADD . ./

EXPOSE 8002
EXPOSE 8008
EXPOSE 8009

CMD [ "sh" ]