default: build

build:
	go build -o main *.go 

test: 
	go test
