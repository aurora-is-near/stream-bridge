FROM golang:1.22-alpine AS build
ENV GOPRIVATE=github.com/aurora-is-near/*
ENV CGO_ENABLED=0

RUN apk add --no-cache git openssh-client

COPY root-config /root/
RUN sed 's|/home/runner|/root|g' -i.bak /root/.ssh/config

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=ssh go mod download -x

COPY . .
ARG APP
RUN --mount=type=ssh go build -o /app cmd/$APP/*.go

FROM alpine:latest
COPY --from=build /app /usr/local/bin/app
RUN addgroup -S aurora && adduser --disabled-password --no-create-home -S aurora -G aurora
USER aurora

ENTRYPOINT [ "/usr/local/bin/app" ]
