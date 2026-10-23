.PHONY: test bench cover lint fmt vet fix fixcheck ci

lint:
	golangci-lint run

test:
	go test -race ./...

bench:
	go test -run='^$$' -bench=. -benchmem -count=5 ./...

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out
	rm -f coverage.out

fmt:
	gofmt -w .

vet:
	go vet ./...

fix:
	go fix ./...

fixcheck:
	go fix -diff ./...

ci:	fmt vet lint fixcheck test
