default:

build:
	mkdir -p dist/
	cd apps/simple && go build -o ../../dist/jet-capture-simple

clean:
	rm -rf dist/