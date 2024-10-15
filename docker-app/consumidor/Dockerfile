FROM bde2020/spark-submit:3.0.0-hadoop3.2

WORKDIR /app
USER root

ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYSPARK_PYTHON=python3

COPY requirements.txt /app
RUN cd /app \
      && pip3 install -r requirements.txt

COPY /app /app

ENTRYPOINT [ "sleep", "infinity" ]