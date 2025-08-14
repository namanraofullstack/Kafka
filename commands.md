# timestamp filtering
Pagination for large results
Stats summary (total scanned, total matched, partitions scanned)

# Powershell cmd to create topics
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic 
test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Produce a message in powershell
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092

# Consume a message in Powershell
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092

# install kafka python to run util file
pip install kafka-python
pip install kafka-python flask

# run as api server
python kafka_util.py --api

# use docker compose exec kafkautil

# to run inside the contaner python commands
docker compose exec kafkautil python kafka_util.py --topic test-topic --indicator error --latest

# fetch the messages 
python kafka_util.py --topic test-topic --indicator error --count 2

# given indicator and source search dump all messages to a file
python kafka_util.py --topic test-topic --indicator error --output matched_messages.txt

# latest messages
python kafka_util.py --topic test-topic --indicator error --latest


# x number of messages
python kafka_util.py --topic test-topic --indicator error --count 5

# api call
http://127.0.0.1:5000/search?topic=test-topic&indicator=error&count=10&latest=trueoutput=errors.txt

# api call for streaming
http://127.0.0.1:5000/stream?topic=test-topic&indicator=error

# to run tests
docker compose exec kafkautil python kafka_util.py --test


# edge cases
Kafka Connection & Configuration

Bootstrap servers unavailable - Connection failures, network timeouts
Topic doesn't exist - Non-existent topic names
Empty topics - Topics with no messages
Consumer timeout - 3-second timeout may be too short for large topics

Message Processing

Encoding issues - Non-UTF-8 messages causing decode errors
Binary/null messages - Messages that can't be decoded to strings
Very large messages - Memory issues with huge message payloads
Empty indicator strings - Searching for empty string matches everything

Memory & Performance

Unbounded memory growth - No limit on matches list size when count is None
Large result sets - Thousands of matches consuming excessive memory
Slow topic consumption - Large topics taking very long to process
Concurrent API requests - Multiple requests creating multiple consumers

File Operations

Output directory permissions - Cannot create/write to output directory
Disk space - Running out of space when writing large files
File overwrite - Silently overwrites existing files
Duplicate file writing - Code writes to both OUTPUT_DIR/filename and ./filename

API Edge Cases

Invalid query parameters - Malformed count values, special characters in topic names
Client disconnection - Streaming endpoint continues processing after client disconnect
Resource cleanup - Consumers not properly closed on API errors
Concurrent access - Thread safety issues with multiple API requests

CLI Edge Cases

Missing required arguments - Running without --topic or --indicator
Invalid argument combinations - Conflicting flags

Data Consistency

Consumer group management - No consumer group specified


# ideas

Case-insensitive search done
regex search or done


# what i did 
show prometheus grafana system metrics
show updated search api
read mesages optmization

docker exec -it djangoapi python manage.py kafkasearch --topic my_topic --indicator "error OR fail" --count 5


docker exec -it djangoapi python manage.py kafkasearch --topic test-topic --indicator "hash" 

docker exec -it djangoapi python manage.py push_test_messages --count 10000 --topic test-topic

Naman.rao@CY-IND-L2207 Kafka % docker exec -i kafka \
  /opt/bitnami/kafka/bin/kafka-console-producer.sh \
  --topic test2-topic \
  --bootstrap-server localhost:9092 \
  --producer-property partitioner.class=org.apache.kafka.clients.producer.internals.DefaultPartitioner \
  --producer-property partitioner.ignore.keys=false \
  --property parse.key=true \
  --property key.separator=":"
