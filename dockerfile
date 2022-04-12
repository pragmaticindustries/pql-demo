FROM python:3.9 as pql_demo_stream
ENV PYTHONUNBUFFERED=1
ADD extract.py /
CMD ["python3.9", "extract.py"]
