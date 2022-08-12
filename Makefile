include .env
DOCKER=$(shell which docker)
ASTRO=$(shell which astro)
IMAGE_NAME="astro-$(shell date +%Y%m%d%H%M%S)"

help:
	@grep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

astro build: ## build a custom astro image that has access to private repo
	$(DOCKER) build -f Dockerfile --progress=plain --build-arg PIP_EXTRA_INDEX_URL=git+https://${PAT}@github.com/astronomer/cse-private-packages.git -t ${IMAGE_NAME} .
	$(ASTRO) dev start --image-name ${IMAGE_NAME}
