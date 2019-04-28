FROM python:3.6

COPY src/ .

RUN pip install -e ./komoot-importer/

ENTRYPOINT ["python", "-m", "komoot-importer.main"]
CMD ["--help"]
