FROM ghcr.io/binkhq/python:3.10-pipenv

WORKDIR /app
ADD main.py settings.py Pipfile Pipfile.lock ./

RUN pipenv install --system --deploy --ignore-pipfile

ENTRYPOINT [ "linkerd-await", "--" ]
CMD ["python", "main.py"]
