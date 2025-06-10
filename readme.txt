COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt\
    && pip install psycopg2-binary sqlalchemy