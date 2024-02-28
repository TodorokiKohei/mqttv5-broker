.PHONY: build run login-aws delete-aws create-aws push-aws register-aws

# コンテナパラメータ
DOCKER_IMAGE := sharedsub
DOCKER_TAG := latest
ALGO := random

# AWS パラメータ
ACCOUNT := 924899176789
VPC_ID := vpc-022dca84fb15b1536
SUBNET_ID := subnet-0ef955c747b1ae558
SECURITY_GROUP_ID := sg-0e64601b05b91d239

# EC2 パラメータ
EC2_NAME := ECS-Broker
EC2_INSTANCE_TYPE := c5.2xlarge
EC2_AMI_ID := ami-0a3cde619b563ae0f
EC2_KEY_NAME := todoroki-aws-lab

# AWS ECS パラメータ
ECS_CLUSTER_NAME := IoTSimulator
ECS_SERVICE_NAME := broker
TASK_DEFINITION_ARN = $(shell aws ecs list-task-definitions --family-prefix "broker" --status ACTIVE --query "taskDefinitionArns[-1]" --output text)

# AWS Cloud Map パラメータ
SERVICE_DISCOVERY_NAMESPACE_NAME := iot-simulator
SERVICE_DISCOVERY_NAMESPACE_ID = $(shell aws servicediscovery list-namespaces --query "Namespaces[?Name=='${SERVICE_DISCOVERY_NAMESPACE_NAME}'].Id" --output text)
SERVICE_DISCOVERY_SERVICE_NAME := broker
SERVICE_DISCOVERY_SERVICE_ID = $(shell aws servicediscovery list-services --query "Services[?Name=='${ECS_SERVICE_NAME}'].Id" --output text | tr -d '\n')
SERVICE_DISCOVERY_SERVICE_ARN = $(shell aws servicediscovery list-services --query "Services[?Name=='${ECS_SERVICE_NAME}'].Arn" --output text | tr -d '\n')

# 実行環境パラメータ
SSH_USER := ec2-user
SSH_KEY := ${HOME}/.ssh/todoroki-aws-lab.pem
OUTPUT_DIR := ${HOME}/mqttv5-aws/broker


# Docker
build: 
	docker build -t sharedsub:${DOCKER_TAG} -f build/Dockerfile --target deploy ./

run:
	docker run -it --rm -p 1883:1883 sharedsub:${DOCKER_TAG} -algo ${ALGO}


# AWS EC2
create-userdata:
	echo "#!/bin/bash" > aws/userdata.sh
	echo "echo "ECS_CLUSTER=${ECS_CLUSTER_NAME}" >> /etc/ecs/ecs.config" >> aws/userdata.sh

create-ec2: create-userdata
	aws ec2 run-instances --image-id ${EC2_AMI_ID}  \
    	--instance-type ${EC2_INSTANCE_TYPE} \
    	--key-name ${EC2_KEY_NAME} \
    	--subnet-id ${SUBNET_ID} \
    	--security-group-ids ${SECURITY_GROUP_ID} \
    	--network-interfaces "DeviceIndex=0,AssociatePublicIpAddress=true" \
    	--block-device-mappings "[{\"DeviceName\":\"/dev/xvdcz\",\"Ebs\":{\"VolumeSize\":30,\"DeleteOnTermination\":true}}]" \
    	--iam-instance-profile Name="ecsInstanceRole" \
    	--user-data "file://aws/userdata.sh" \
    	--tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${EC2_NAME},{Key=Owner,Value=todoroki}]" \
    	--count 1 --no-cli-pager


# AWS ECR
login-ecr:
	aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin ${ACCOUNT}.dkr.ecr.ap-northeast-1.amazonaws.com

create-repo: login-ecr
	aws ecr create-repository --repository-name ${DOCKER_IMAGE} --region ap-northeast-1

delete-repo: login-ecr
	aws ecr delete-repository --repository-name ${DOCKER_IMAGE} --region ap-northeast-1 --force
	
push: build login-ecr
	docker tag ${DOCKER_IMAGE}:${DOCKER_TAG} ${ACCOUNT}.dkr.ecr.ap-northeast-1.amazonaws.com/${DOCKER_IMAGE}:${DOCKER_TAG}
	docker push ${ACCOUNT}.dkr.ecr.ap-northeast-1.amazonaws.com/${DOCKER_IMAGE}:${DOCKER_TAG}


# AWS Cloud Map
create-service-discovery-namespace:
	aws servicediscovery create-private-dns-namespace --name ${SERVICE_DISCOVERY_NAMESPACE_NAME} --vpc ${VPC_ID} --query "OperationId" --output text > /tmp/operation-id
	@echo "Waiting for namespace to be created..."
	@while [ `aws servicediscovery get-operation --operation-id $$(cat /tmp/operation-id) --query "Operation.Status" --output text` = "PENDING" ]; do sleep 5; done
	@rm /tmp/operation-id

create-service-discovery-service:
	aws servicediscovery create-service --name ${SERVICE_DISCOVERY_SERVICE_NAME} --dns-config "NamespaceId="${SERVICE_DISCOVERY_NAMESPACE_ID}",DnsRecords=[{Type="A",TTL="300"}]" --health-check-custom-config FailureThreshold=1 --no-cli-pager


# AWS ECS
create-cluster:
	aws ecs create-cluster --cluster-name ${ECS_CLUSTER_NAME} --no-cli-pager

register-task:
	aws ecs register-task-definition --cli-input-json file://aws/broker-task.json --no-cli-pager

start-service:
	aws ecs create-service --cluster ${ECS_CLUSTER_NAME} --service-name ${ECS_SERVICE_NAME} --task-definition ${TASK_DEFINITION_ARN} \
		--desired-count 1 \
		--network-configuration "awsvpcConfiguration={subnets=${SUBNET_ID},securityGroups=${SECURITY_GROUP_ID}}" \
		--service-registries registryArn=$(SERVICE_DISCOVERY_SERVICE_ARN) \
		--launch-type EC2 --no-cli-pager
	@echo "Waiting for service to be created"
	@while [ `aws ecs describe-services --cluster ${ECS_CLUSTER_NAME} --services ${ECS_SERVICE_NAME} --query "services[0].runningCount"` -ne 1 ]; do sleep 5; done

stop-service:
	aws ecs update-service --cluster ${ECS_CLUSTER_NAME} --service ${ECS_SERVICE_NAME} --desired-count 0 --no-cli-pager
	aws ecs delete-service --cluster ${ECS_CLUSTER_NAME} --service ${ECS_SERVICE_NAME} --no-cli-pager
	@echo "Waiting for service to be deleted..."
	@while [ `aws ecs describe-services --cluster ${ECS_CLUSTER_NAME} --services ${ECS_SERVICE_NAME} --query "services[0].status" --output text` != "INACTIVE" ]; do sleep 5; done

delete-service-discovery-service:
	aws servicediscovery delete-service --id ${SERVICE_DISCOVERY_SERVICE_ID} --no-cli-pager

delete-service-discovery-namespace:
	aws servicediscovery delete-namespace --id ${SERVICE_DISCOVERY_NAMESPACE_ID} --query "OperationId" --output text --no-cli-pager > /tmp/operation-id
	@echo "Waiting for namespace to be deleted.."
	@while [ `aws servicediscovery get-operation --operation-id $$(cat /tmp/operation-id) --query "Operation.Status" --output text` = "SUCCESS" ]; do sleep 10; done
	@rm /tmp/operation-id


# Operation to build the experimental environment
setup-ecs-service:
	@echo "Setting up ECS Service..."
	@$(MAKE) create-service-discovery-namespace
	@$(MAKE) create-service-discovery-service
	@$(MAKE) start-service

teardown-ecs-service:
	@echo "Tearing down ECS Service..."
	@$(MAKE) stop-service
	@$(MAKE) delete-service-discovery-service
	@$(MAKE) delete-service-discovery-namespace
	@$(MAKE) get-service-results

get-service-results:
	$(eval TIMESTAMP := $(shell date "+%Y%m%d_%H%M%S"))
	@aws ec2 describe-instances \
		--query "Reservations[*].Instances[?State.Name=='running'].{IP:PublicIpAddress,Name:Tags[?Key=='Name'].Value | [0]}" \
		--filters "Name=tag:Owner,Values=todoroki" "Name=tag:Name,Values=*Broker*" "Name=instance-state-name,Values=running" \
		--output text | \
	while read ip name ; do \
		echo "IP: $$ip, Name: $$name" ; \
		mkdir -p ${OUTPUT_DIR}/${TIMESTAMP}/$$name && \
		scp -i ${SSH_KEY} -o StrictHostKeyChecking=no -r ${SSH_USER}@$$ip:/tmp/results/* ${OUTPUT_DIR}/${TIMESTAMP}/$$name && \
		ssh -i ${SSH_KEY} -n ${SSH_USER}@$$ip "sudo rm -rf /tmp/results/*" ; \
	done
	@cp -r aws/* ${OUTPUT_DIR}/${TIMESTAMP}/
#
#echo:
#	@cat aws/broker-task.json | \
# 		jq '.containerDefinitions[0].image|="${ACCOUNT}.dkr.ecr.ap-northeast-1.amazonaws.com/${DOCKER_IMAGE}:${DOCKER_TAG}"' | \
# 		jq '.executionRoleArn|="arn:aws:iam::${ACCOUNT}:role/ecsTaskExecutionRole"'