FROM python:3.9

LABEL version="1.0"

WORKDIR C:\Users\luchi\Base de datos\python\entregable

COPY ExtracAPI.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD [ "python", "-u", "ExtracAPI.py"]







