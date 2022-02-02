FROM golang:alpine as builder
WORKDIR $GOPATH/src/github.com/NikeNano/postrgres-queue/
COPY . . 
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -a -o server ./server/

#https://medium.com/analytics-vidhya/grpc-service-with-docker-c584e93343c0
# generate clean, final image for end users
FROM alpine:3.11.3
#FROM ubuntu:latest
WORKDIR /root/
COPY --from=builder $GOPATH/go/src/github.com/NikeNano/postrgres-queue/server/server .
CMD ["./server"]