# compile stage
FROM golang:1.12.4 AS build

COPY . /src

WORKDIR /src/
ENTRYPOINT [ "make" ]
CMD [ "test-all" ]