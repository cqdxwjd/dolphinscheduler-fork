docker.hub=hub.hmf.xyz
docker.tag=3.1.0

push-images:
	docker push $(docker.hub)/emr/dolphinscheduler-standalone-server:$(docker.tag)
	docker push $(docker.hub)/emr/dolphinscheduler-standalone-server:latest
	docker push $(docker.hub)/emr/dolphinscheduler-alert-server:$(docker.tag)
	docker push $(docker.hub)/emr/dolphinscheduler-alert-server:latest
	docker push $(docker.hub)/emr/dolphinscheduler-master:$(docker.tag)
	docker push $(docker.hub)/emr/dolphinscheduler-master:latest
	docker push $(docker.hub)/emr/dolphinscheduler-worker:$(docker.tag)
	docker push $(docker.hub)/emr/dolphinscheduler-worker:latest
	docker push $(docker.hub)/emr/dolphinscheduler-api:$(docker.tag)
	docker push $(docker.hub)/emr/dolphinscheduler-api:latest
	docker push $(docker.hub)/emr/dolphinscheduler-tools:$(docker.tag)
	docker push $(docker.hub)/emr/dolphinscheduler-tools:latest
