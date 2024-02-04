.PHONY: build run login-aws delete-aws create-aws push-aws register-aws

DOCKER_IMAGE := sharedsub
DOCKER_TAG := latest
ALGO := random

AWS_ECR_REGISTRY := 924899176789.dkr.ecr.ap-northeast-1.amazonaws.com


build: 
	docker build -t sharedsub:${DOCKER_TAG} -f build/Dockerfile --target deploy ./

run:
	docker run -it --rm -p 1883:1883 sharedsub:${DOCKER_TAG} -algo ${ALGO}


# AWS Operation
login-aws:
	aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin ${AWS_ECR_REGISTRY}

create-aws:
	aws ecr create-repository --repository-name ${DOCKER_IMAGE} --region ap-northeast-1

delete-aws:
	aws ecr delete-repository --repository-name ${DOCKER_IMAGE} --region ap-northeast-1 --force
	
push-aws: build
	docker tag ${DOCKER_IMAGE}:${DOCKER_TAG} ${AWS_ECR_REGISTRY}/${DOCKER_IMAGE}:${DOCKER_TAG}
	docker push ${AWS_ECR_REGISTRY}/${DOCKER_IMAGE}:${DOCKER_TAG}	

register-aws:
	aws ecs register-task-definition --cli-input-json file://aws/broker-task.json --no-cli-pager

