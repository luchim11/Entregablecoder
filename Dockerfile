FROM python:3.9

LABEL version="1.0"

WORKDIR /home/app

COPY ExtracAPI.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD [ "python", "-u", "ExtracAPI.py"]







