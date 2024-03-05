FROM python:3.10-slim

# Change working directory
WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app
ENV pg_host=${pg_host}
ENV pg_port=${pg_port}
ENV pg_db=${pg_db}
ENV pg_user=${pg_user}
ENV pg_password=${pg_password}

# SpaceDevs
ENV space_devs_token=${space_devs_token}

# Ghost Token
ENV ghost_api_token=${ghost_api_token}

# Install dependencies
COPY ./requirements.txt .
RUN pip install -r requirements.txt

# Copy source code
#COPY ./dagster.yaml ./workspace.yaml ./
COPY  . .

CMD ["dagit", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]
