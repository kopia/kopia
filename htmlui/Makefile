all:

include ../tools/tools.mk

dev: node_modules
	$(npm) $(npm_flags) run start

# build HTML, run in CI mode which is more strict
build-html: node_modules
	CI=true $(npm) $(npm_flags) run build

node_modules: $(npm)
	$(npm) $(npm_flags) install

clean:
	rm -rf node_modules build