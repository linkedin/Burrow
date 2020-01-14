.PHONY: get update fmt lint test

GO       := GO111MODULE=on GOPRIVATE=github.com/linkedin GOSUMDB=off go
GOBUILD  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG)
GOTEST   := $(GO) test -gcflags='-l' -p 3 -v -race

FILES    := $(shell find core -name '*.go' -type f -not -name '*.pb.go' -not -name '*_generated.go' -not -name '*_test.go')
TESTS    := $(shell find core -name '*.go' -type f -not -name '*.pb.go' -not -name '*_generated.go' -name '*_test.go')

get:
	$(GO) get ./...
	$(GO) mod verify
	$(GO) mod tidy

update:
	$(GO) get -u -v all
	$(GO) mod verify
	$(GO) mod tidy

fmt:
	gofmt -s -l -w $(FILES) $(TESTS)

lint:
	golangci-lint run

test:
	$(GOTEST) ./...
