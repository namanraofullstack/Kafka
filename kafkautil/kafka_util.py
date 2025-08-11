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

    # Seek to scan_limit messages before latest
    for tp in topic_partitions:
        start_offset = max(end_offsets[tp] - scan_limit, 0)
        consumer.seek(tp, start_offset)

    matches = []
    scanned = 0

    for message in consumer:
        scanned += 1
        msg_value = message.value.decode('utf-8')
        if indicator in msg_value:
            matches.append(msg_value)
            if count and len(matches) >= count:
                break
        if scanned >= scan_limit:
            break

    consumer.close()
    return [matches[-1]] if latest and matches else matches


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


if __name__ == '__main__':
    print("Starting Flask API server at http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000)
