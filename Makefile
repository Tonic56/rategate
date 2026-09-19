.PHONY: test lint fmt vet ci

lint:
	golangci-lint run

test:
	go test -race ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

ci:	fmt vet lint test
