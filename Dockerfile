FROM python:3.6

COPY src/ .

RUN pip install -e ./kmt2es/

ENTRYPOINT ["python", "-m", "kmt2es.main"]
CMD ["--help"]
