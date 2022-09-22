FROM python:3.7-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app
COPY . /opt/dagster/app/
COPY . /opt/dagster/dagster_home/
RUN cd /opt/dagster/app/; pip install -e .

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

# Copy dagster instance YAML to $DAGSTER_HOME
# COPY dagster.yaml /opt/dagster/dagster_home/

WORKDIR /opt/dagster/app

EXPOSE 3000

CMD ["dagit", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]
