FROM golang:1.22 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /kvnode ./cmd/kvnode

FROM gcr.io/distroless/static-debian12
COPY --from=build /kvnode /kvnode
ENTRYPOINT ["/kvnode"]
