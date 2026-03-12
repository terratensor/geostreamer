# Версия из git tag или dev
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS = -ldflags "-X github.com/terratensor/geostreamer/internal/version.Version=$(VERSION) \
                    -X github.com/terratensor/geostreamer/internal/version.Commit=$(COMMIT) \
                    -X github.com/terratensor/geostreamer/internal/version.BuildTime=$(BUILD_TIME)"

.PHONY: build
build:
	go build $(LDFLAGS) -o geostreamer cmd/geostreamer/main.go

.PHONY: install
install:
	go install $(LDFLAGS) ./cmd/geostreamer

.PHONY: clean
clean:
	rm -f geostreamer

.PHONY: version
version:
	@echo "Version: $(VERSION)"
	@echo "Commit: $(COMMIT)"
	@echo "Build time: $(BUILD_TIME)"