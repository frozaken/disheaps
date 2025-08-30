.PHONY: generate clean test

# Generate Go code from proto files
generate:
	buf generate

# Clean generated files
clean:
	rm -rf ../disheap

# Lint proto files
lint:
	buf lint

# Format proto files
format:
	buf format -w

# Test proto compilation
test:
	buf build
	buf lint
	$(MAKE) generate
	go mod tidy -C ..
	go build -C .. .

# Install dependencies (buf)
deps:
	@which buf > /dev/null || (echo "Installing buf..." && \
		curl -sSL "https://github.com/bufbuild/buf/releases/latest/download/buf-$(shell uname -s)-$(shell uname -m)" -o /tmp/buf && \
		chmod +x /tmp/buf && \
		sudo mv /tmp/buf /usr/local/bin/buf)
