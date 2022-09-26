default:

build:
	mkdir -p dist/
	cd apps/ndjson && go build -o ../../dist/jet-capture-ndjson

clean:
	rm -rf dist/

test:
	go test -v ./...
