import argparse
from kafka import KafkaConsumer
from flask import Flask, request, jsonify, Response
import sys
import os

app = Flask(__name__)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', './output')


# ======================
# Core Kafka Utilities
# ======================

def search_messages(topic, indicator, bootstrap_servers=BOOTSTRAP_SERVERS, count=None, latest=False):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        consumer_timeout_ms=3000
    )
    matches = []

    for message in consumer:
        msg_value = message.value.decode('utf-8')
        if indicator in msg_value:
            matches.append(msg_value)
            if count and len(matches) > count:
                matches.pop(0)  # keep last count messages

    consumer.close()
    return [matches[-1]] if latest and matches else matches


def dump_messages_to_file(messages, filename):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    print(f"Dumping {len(messages)} messages to file: {path}")
    with open(path, 'w', encoding='utf-8') as f:
        for m in messages:
            f.write(m + '\n')


def stream_messages(topic, indicator, bootstrap_servers=BOOTSTRAP_SERVERS):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        consumer_timeout_ms=3000
    )
    try:
        for message in consumer:
            msg_value = message.value.decode('utf-8')
            if indicator in msg_value:
                yield msg_value + '\n'
    finally:
        consumer.close()


# ======================
# Flask API Endpoints
# ======================

@app.route('/search', methods=['GET'])
def api_search():
    topic = request.args.get('topic')
    indicator = request.args.get('indicator')
    count = request.args.get('count', type=int)
    latest = request.args.get('latest', default='false').lower() == 'true'
    output = request.args.get('output')

    if not topic or not indicator:
        return jsonify({"error": "Missing required parameters"}), 400

    try:
        messages = search_messages(topic, indicator, count=count, latest=latest)
    except Exception as e:
        return jsonify({"error": f"Kafka error: {str(e)}"}), 500

    if output:
        dump_messages_to_file(messages, output)

    return jsonify({
        "matched_count": len(messages),
        "messages": messages
    })


@app.route('/stream', methods=['GET'])
def api_stream():
    topic = request.args.get('topic')
    indicator = request.args.get('indicator')

    if not topic or not indicator:
        return jsonify({"error": "Missing required parameters"}), 400

    def generate():
        for msg in stream_messages(topic, indicator):
            yield msg

    return Response(generate(), mimetype='text/plain')


# ======================
# CLI + Tests
# ======================

import os
import tempfile
from unittest.mock import patch, MagicMock

def run_tests():
    print("[TEST] Starting Kafka utility edge case checks...\n")

    # Test configuration
    LARGE_MESSAGE_COUNT = 500         # For dump test
    STREAM_MESSAGE_COUNT = 1000      # For search test
    MATCH_INTERVAL = 10                # Every nth message has "error"

    # Temp directory for test artifacts
    temp_dir = tempfile.mkdtemp(prefix="kafka_test_")

    # 1. Large result set memory handling (dump stress)
    try:
        print(f"\n[TEST] 1. Large result set file dump ({LARGE_MESSAGE_COUNT} messages)...")

        # Create mock Kafka messages
        mock_messages = []
        for i in range(LARGE_MESSAGE_COUNT):
            mock_msg = MagicMock()
            mock_msg.value.decode.return_value = f"test message {i} with indicator"
            mock_messages.append(mock_msg)

        # Mock KafkaConsumer
        with patch('kafka.KafkaConsumer') as mock_consumer:
            mock_consumer_instance = MagicMock()
            mock_consumer_instance.__iter__.return_value = iter(mock_messages)
            mock_consumer.return_value = mock_consumer_instance

            # Test search_messages with large dataset
            messages = search_messages("test_topic", "indicator")
            assert len(messages) == LARGE_MESSAGE_COUNT, f"Expected {LARGE_MESSAGE_COUNT}, got {len(messages)}"

            # Test file dump
            large_file = os.path.join(temp_dir, "large_test.txt")
            dump_messages_to_file(messages, large_file)
            assert os.path.exists(large_file), "Large file was not created"

            with open(large_file, 'r') as f:
                lines = f.readlines()
            assert len(lines) == LARGE_MESSAGE_COUNT, f"Expected {LARGE_MESSAGE_COUNT} lines, got {len(lines)}"

        print(f"    ✅ Successfully handled and dumped {LARGE_MESSAGE_COUNT} messages")
    except Exception as e:
        print(f"    ❌ Failed during large set handling: {e}")

    # 2. Search returning large matches (search stress, streaming)
    try:
        print(f"\n[TEST] 2. Streaming dataset ({STREAM_MESSAGE_COUNT} messages) with filtered matches...")

        # Create mock messages, inserting "error" every MATCH_INTERVAL
        mock_messages = []
        expected_matches = 0
        for i in range(STREAM_MESSAGE_COUNT):
            msg_content = f"message {i}"
            if i % MATCH_INTERVAL == 0:
                msg_content += " error occurred"
                expected_matches += 1

            mock_msg = MagicMock()
            mock_msg.value.decode.return_value = msg_content
            mock_messages.append(mock_msg)

        with patch('kafka.KafkaConsumer') as mock_consumer:
            mock_consumer_instance = MagicMock()
            mock_consumer_instance.__iter__.return_value = iter(mock_messages)
            mock_consumer.return_value = mock_consumer_instance

            # Streaming verification
            streamed_messages = [msg.strip() for msg in stream_messages("test_topic", "error")]
            assert len(streamed_messages) == expected_matches, \
                f"Expected {expected_matches}, got {len(streamed_messages)}"
            assert all("error" in msg for msg in streamed_messages), "Found a non-error message in results"

            # Count limit test
            limited_messages = search_messages("test_topic", "error", count=100)
            assert len(limited_messages) == 100, f"Expected 100 messages, got {len(limited_messages)}"

            # Latest flag test
            latest_message = search_messages("test_topic", "error", latest=True)
            assert len(latest_message) == 1 and "error" in latest_message[0], "Latest message test failed"

        print(f"    ✅ Search & streaming test passed with {expected_matches} matches")
    except Exception as e:
        print(f"    ❌ Failed during large search handling: {e}")

    # Cleanup
    try:
        import shutil
        shutil.rmtree(temp_dir)
        print("\n[TEST] Cleanup successful — temporary files removed")
    except Exception as e:
        print(f"    ⚠️ Cleanup failed: {e}")

    print("\n[TEST] All edge case tests completed!")



def cli():
    if '--api' in sys.argv:
        print("Starting Flask API server at http://0.0.0.0:5000")
        app.run(host="0.0.0.0", port=5000)
        return

    if '--test' in sys.argv:
        run_tests()
        return

    parser = argparse.ArgumentParser(description='Kafka Search Utility')
    parser.add_argument('--topic', required=True)
    parser.add_argument('--indicator', required=True)
    parser.add_argument('--count', type=int)
    parser.add_argument('--latest', action='store_true')
    parser.add_argument('--output', help='Dump matched messages to file')

    args = parser.parse_args()

    messages = search_messages(args.topic, args.indicator, count=args.count, latest=args.latest)

    if args.output:
        dump_messages_to_file(messages, args.output)

    if messages:
        print(f"Found {len(messages)} messages:")
        for m in messages:
            print(m)
    else:
        print("No matching messages found.")


if __name__ == '__main__':
    cli()
