.PHONY:build run

DOCKER_TAG := latest
ALGO := random
build: ## Build docker image to deploy
	docker build -t sharedsub:${DOCKER_TAG} -f build/Dockerfile --target deploy ./

run:
	docker run -it --rm -p 1883:1883 sharedsub:${DOCKER_TAG} -algo ${ALGO}