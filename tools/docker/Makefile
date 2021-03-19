DOCKER_BUILD_DIR=.

make:
	cp -rv $(CURDIR)/../dist/kopia_linux_amd64/ $(DOCKER_BUILD_DIR)/bin-amd64
	cp -rv $(CURDIR)/../dist/kopia_linux_arm64/ $(DOCKER_BUILD_DIR)/bin-arm64
	cp -rv $(CURDIR)/../dist/kopia_linux_arm_6/ $(DOCKER_BUILD_DIR)/bin-arm
	docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v6 -t containers.jkowalski.net/kopia:latest --push $(DOCKER_BUILD_DIR)