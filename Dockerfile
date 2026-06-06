FROM apache/airflow:2.9.3-python3.10

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-kafka==1.6.1 \
    confluent-kafka \
    kafka-python \
    xmltodict \
    openpyxl \
    paramiko \
    boto3 \
    pypdf