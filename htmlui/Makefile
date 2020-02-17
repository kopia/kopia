all:

include ../tools/tools.mk

dev: node_modules
	$(npm) $(npm_flags) run start

build-html: node_modules
	$(npm) $(npm_flags) run build

node_modules: $(npm)
	$(npm) $(npm_flags) install

clean:
	rm -rf node_modules build

test:
	$(npm) $(npm_flags) test