APP_NAME=burrow

QUAY_REPO?="unknown"
QUAY_USERNAME?="unknown"
QUAY_PASSWORD?="unknown"

CIRCLE_BUILD_NUM?="unknown"
VERSION=1.3.3-$(CIRCLE_BUILD_NUM)

IMAGE = quay.io/$(QUAY_REPO)/$(APP_NAME):$(VERSION)

build:
	docker build -t $(IMAGE) .

login:
	docker login -u $(QUAY_USERNAME) -p $(QUAY_PASSWORD) quay.io

logout:
	docker logout

push:
	docker push $(IMAGE)
	docker rmi $(IMAGE)

scan:
	trivy --clear-cache --severity "UNKNOWN,MEDIUM,HIGH,CRITICAL" --exit-code 0 --quiet --auto-refresh $(IMAGE)
