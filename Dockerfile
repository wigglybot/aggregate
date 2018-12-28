FROM python:3.7-alpine as base

FROM base as builder

RUN mkdir /install
ADD app /install
WORKDIR /install

RUN apk add gcc musl-dev --no-cache
RUN pip install --install-option="--prefix=/install" --ignore-installed -r requirements.txt

FROM base
COPY --from=builder /install /usr/local
COPY app /app

WORKDIR /app
RUN chmod +x ./wait-for

CMD ["./wait-for", "eventstore:2113", "--", "python", "component/app.py"]