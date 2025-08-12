import argparse
from kafka import KafkaConsumer, TopicPartition
from flask import Flask, request, jsonify
import os

app = Flask(__name__)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', './output')

def search_messages(topic, indicator, bootstrap_servers=BOOTSTRAP_SERVERS, count=None, latest=False, scan_limit=100):
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        consumer_timeout_ms=3000
    )

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise Exception(f"No partitions found for topic: {topic}")

    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)

    # Get latest offsets
    end_offsets = consumer.end_offsets(topic_partitions)

    if latest or count == 1:
        # For latest match or when count=1, scan from most recent backwards
        matches = []
        
        for tp in topic_partitions:
            partition_matches = []
            current_offset = end_offsets[tp] - 1
            start_offset = max(end_offsets[tp] - scan_limit, 0)
            
            # Scan backwards from latest
            for offset in range(current_offset, start_offset - 1, -1):
                if offset < 0:
                    break
                    
                consumer.seek(tp, offset)
                try:
                    message = next(consumer)
                    msg_value = message.value.decode('utf-8')
                    if indicator in msg_value:
                        partition_matches.append((message.timestamp, msg_value))
                        break  # Found the latest match in this partition
                except StopIteration:
                    break
            
            matches.extend(partition_matches)
        
        # Sort by timestamp descending and take the most recent
        if matches:
            matches.sort(key=lambda x: x[0], reverse=True)
            consumer.close()
            return [matches[0][1]]  # Return just the message content
        else:
            consumer.close()
            return []
    else:
        # For regular searches, collect matches with timestamps for proper sorting
        matches_with_timestamps = []
        scanned = 0
        
        # Seek to scan_limit messages before latest for each partition
        for tp in topic_partitions:
            start_offset = max(end_offsets[tp] - scan_limit, 0)
            consumer.seek(tp, start_offset)

        for message in consumer:
            scanned += 1
            msg_value = message.value.decode('utf-8')
            if indicator in msg_value:
                matches_with_timestamps.append((message.timestamp, msg_value))
                if count and len(matches_with_timestamps) >= count:
                    break
            if scanned >= scan_limit:
                break

        # Sort by timestamp descending (newest first) and extract just the messages
        matches_with_timestamps.sort(key=lambda x: x[0], reverse=True)
        matches = [msg for _, msg in matches_with_timestamps]
        
        consumer.close()
        return matches

def dump_messages_to_file(messages, filename):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    print(f"Dumping {len(messages)} messages to file: {path}")
    with open(path, 'w', encoding='utf-8') as f:
        for m in messages:
            f.write(m + '\n')

@app.route('/search', methods=['GET'])
def api_search():
    topic = request.args.get('topic')
    indicator = request.args.get('indicator')
    count = request.args.get('count', type=int)
    latest = request.args.get('latest', default='false').lower() == 'true'
    scan_limit = request.args.get('scan_limit', type=int, default=100)
    output = request.args.get('output')

    if not topic or not indicator:
        return jsonify({"error": "Missing required parameters"}), 400

    try:
        messages = search_messages(topic, indicator, count=count, latest=latest, scan_limit=scan_limit)
    except Exception as e:
        return jsonify({"error": f"Kafka error: {str(e)}"}), 500

    if output:
        dump_messages_to_file(messages, output)

    return jsonify({
        "matched_count": len(messages),
        "messages": messages
    })

def cli_interface():
    parser = argparse.ArgumentParser(description="Search Kafka messages from CLI")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--indicator", required=True, help="String to search for")
    parser.add_argument("--count", type=int, help="Number of matches to return")
    parser.add_argument("--latest", action="store_true", help="Return only latest match")
    parser.add_argument("--scan_limit", type=int, default=100, help="Number of messages to scan from latest")
    parser.add_argument("--output", help="File to dump results")

    args = parser.parse_args()

    try:
        messages = search_messages(
            args.topic,
            args.indicator,
            count=args.count,
            latest=args.latest,
            scan_limit=args.scan_limit
        )

        if args.output:
            dump_messages_to_file(messages, args.output)
        else:
            for m in messages:
                print(m)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] != "runserver":
        cli_interface()
    else:
        print("Starting Flask API server at http://0.0.0.0:5000")
        app.run(host="0.0.0.0", port=5000)