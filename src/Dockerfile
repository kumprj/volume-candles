FROM python:3.7

WORKDIR /usr/local/bin

COPY requirements.txt .
COPY createcandle.py .
COPY credentials.py .
COPY settings.yaml .

RUN cat requirements.txt | xargs -n 1 python3 -m pip install

# For when it is migrated to AWS.
# ARG aws_id=default_value
# ENV AWS_ACCESS_KEY_ID=$aws_id

# ARG aws_key=default_value
# ENV AWS_SECRET_ACCESS_KEY=$aws_key

CMD python3 createcandle.py