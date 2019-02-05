FROM golang:1.11-alpine AS base

# Install dependencies
RUN apk --update --no-cache add git curl

ARG DEP_VERSION=0.5.0

# Download dep binary to bin folder in $GOPATH
RUN mkdir -p /usr/local/bin \
    && curl -fsSL -o /usr/local/bin/dep https://github.com/golang/dep/releases/download/v${DEP_VERSION}/dep-linux-amd64 \
    && chmod +x /usr/local/bin/dep

WORKDIR /go/src/github.com/alerting/alerts-naads
COPY . /go/src/github.com/alerting/alerts-naads

RUN dep ensure
RUN CGO_ENABLED=0 GOOS=linux go install .

FROM alpine:3.9
COPY --from=base /go/bin/alerts-naads /alerts-naads
USER 10000
ENTRYPOINT ["/alerts-naads"]
