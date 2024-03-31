FROM jupyter/scipy-notebook:latest

LABEL version="1.0"

WORKDIR C:\Users\luchi\Base de datos\python\entregable

COPY ExtracAPI.ipynb .

CMD [ "python", "-u", "ExtracAPI.ipynb"]







