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

