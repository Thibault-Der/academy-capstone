# syntax=docker/dockerfile:1

FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
WORKDIR /app
COPY /connector2.py .
USER 0
RUN pip install pyspark==3.1.2 boto3
CMD python3 connector2.py
