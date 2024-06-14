FROM golang:1.18-alpine AS build
ENV GOPRIVATE=github.com/aurora-is-near/*
ENV CGO_ENABLED=0

RUN apk add --no-cache git openssh-client

COPY root-config /root/
RUN sed 's|/home/runner|/root|g' -i.bak /root/.ssh/config

RUN ls -laR /root/
RUN cat /root/.gitconfig
RUN cat /root/.ssh/config

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=ssh go mod download -x

COPY . .
ARG APP
RUN --mount=type=ssh go build -o /$APP cmd/$APP/*.go

FROM alpine:latest
ARG APP
COPY --from=build /$APP /usr/local/bin/$APP
RUN addgroup -S aurora && adduser --disabled-password --no-create-home -S aurora -G aurora
USER aurora

ENTRYPOINT [ "/usr/local/bin/$APP" ]
