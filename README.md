# CSCIE88c - Programming in Scala for Big Data Systems (Final Project)

## Usage

```shell
make start-kafka
make stop-kafka
```

```shell
make start-spark
make stop-spark
```


### Resources

- [Creating a Spark Standalone Cluster with Docker and docker-compose](https://dev.to/mvillarrealb/creating-a-spark-standalone-cluster-with-docker-and-docker-compose-2021-update-6l4)


### Limitations

- It is not possible to do more than one aggregation in a given streaming application. Spark Streaming 
appears to still be relatively experimental or not fully feature-complete.

### Notes

- Reset vm.max_map_count
```shell
sudo sysctl -w vm.mac_map_count=65530
```