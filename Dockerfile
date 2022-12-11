FROM python:3.7

RUN pip3 install pandas pyarrow

WORKDIR /usr/src/app

COPY main.py ./

ENTRYPOINT [ "python", "./main.py" ]