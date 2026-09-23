.PHONY: test lint fmt vet fix fixcheck ci

lint:
	golangci-lint run

test:
	go test -race ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

fix:
	go fix ./...

fixcheck:
	go fix -diff ./...

ci:	fmt vet lint fixcheck test
