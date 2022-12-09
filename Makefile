ELK_VERSION := 8.5.2-amd64

start-kafka:
	@docker-compose up -d kafka-ui kafka zookeeper
	@echo "Kafka UI: http://localhost:8080"

stop-kafka:
	@docker-compose down kafka-ui kafka zookeeper

start-spark:
	@docker-compose up -d spark-master spark-worker
	@echo "Spark Master: http://localhost:9090"
	@echo "Spark Worker: http://localhost:9091"

stop-spark:
	@docker-compose down spark-master spark-worker

start-elk:
	@docker-compose up -d setup elasticsearch kibana logstash

stop-elk:
	@docker-compose down elasticsearch kibana logstash

pdf:
	@pandoc csci-e88c_final_project.md -o final_project.pdf --from markdown --template eisvogel --toc --listings --number-sections