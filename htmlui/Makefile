all:

include ../tools/tools.mk

build-html: build/index.html

deps: node_modules/.up-to-date

install:
	$(npm) $(npm_flags) install --no-audit
	$(npm) $(npm_flags) audit --production

dev: node_modules/.up-to-date
	$(npm) $(npm_flags) run start

build/index.html: node_modules/.up-to-date src/* public/*
	$(npm) $(npm_flags) run build

node_modules/.up-to-date: $(npm) package.json package-lock.json
	$(npm) $(npm_flags) install --no-audit
	$(npm) $(npm_flags) audit --production
	echo updated > node_modules/.up-to-date

clean:
	rm -rf node_modules build

test:
	$(npm) $(npm_flags) test
