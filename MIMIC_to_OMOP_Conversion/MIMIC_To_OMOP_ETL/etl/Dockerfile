from ghcr.io/chorus-ai/chorus-etl:main

RUN rm -rf /opt/etl && mkdir /opt/etl && mkdir /ares
WORKDIR /opt/etl
COPY . .

# ENTRYPOINT ["python3", "check_db.py"]
ENTRYPOINT ["python3", "-c", "from etl.mimic import mimic_etl; mimic_etl()"]