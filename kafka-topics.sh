#!/bin/bash

KAFKA=localhost:9092

# Main topics
docker exec kafka kafka-topics --create --topic order.created --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic stock.reserved --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic stock.reservation.failed --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic payment.completed --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic payment.failed --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic stock.released --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic order.cancelled --bootstrap-server $KAFKA --partitions 3 --replication-factor 1

# Dead Letter Topics
docker exec kafka kafka-topics --create --topic order.created.DLT --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic stock.reserved.DLT --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic stock.reservation.failed.DLT --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic payment.completed.DLT --bootstrap-server $KAFKA --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic payment.failed.DLT --bootstrap-server $KAFKA --partitions 3 --replication-factor 1

echo "All topics created."