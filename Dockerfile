FROM apache/airflow:2.6.3-python3.10

USER root

# Cài đặt Chrome và dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable=137.0.7151.55-1 \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt các dependencies khác
RUN apt-get update && apt-get install -y \
    xvfb \
    fluxbox \
    x11vnc \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Cài đặt Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install  -r /tmp/requirements.txt\
    && pip install psycopg2-binary sqlalchemy pymysql