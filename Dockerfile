# syntax=docker/dockerfile:1

FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
WORKDIR /app
COPY /connector.py .
USER root
RUN pip install pyspark==3.1.2
CMD python3 connector.py
