FROM python:3.7

WORKDIR /usr/local/bin

COPY src/requirements.txt .
COPY src/createcandle.py .
COPY src/credentials.py .
COPY src/settings_actual.yaml .

RUN cat requirements.txt | xargs -n 1 python3 -m pip install

# For when it is migrated to AWS.
# ARG aws_id=default_value
# ENV AWS_ACCESS_KEY_ID=$aws_id

# ARG aws_key=default_value
# ENV AWS_SECRET_ACCESS_KEY=$aws_key

CMD python3 createcandle.py