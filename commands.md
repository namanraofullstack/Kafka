# Produce a message in powershell
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092

# Consume a message in Powershell
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092

# searching in tool
docker exec -it djangoapi python manage.py kafkasearch --topic test-topic --indicator "hash" 

# testing in tool
docker exec -it djangoapi python manage.py push_test_messages --count 10000 --topic test-topic

# to run on ec2
docker run -it --rm --network=ctix-v3 -e BOOTSTRAP_SERVER=broker:29092 packages.cyware.com/ctix/naman/kafka-search-tool:latest /bin/bash

# to run on ec2 without kafget
python manage.py kafkasearch --list_topics

# pull for docker tool in ec2
docker pull packages.cyware.com/ctix/naman/kafka-search-tool:latest
