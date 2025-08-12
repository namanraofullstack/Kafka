import argparse
from kafka import KafkaConsumer, TopicPartition
from flask import Flask, request, jsonify
import os
import re
import sys

app = Flask(__name__)

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', './output')

class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def colorize_text(text, color):
    """Add color to text for terminal output"""
    return f"{color}{text}{Colors.END}"

def highlight_indicator(message, indicator):
    """Highlight the indicator(s) in red within the message"""
    # Handle AND logic
    if '&' in indicator or re.search(r'\bAND\b', indicator, re.IGNORECASE):
        indicators = re.split(r'\s*&\s*|\s+AND\s+', indicator, flags=re.IGNORECASE)
        result = message
        for ind in indicators:
            ind = ind.strip()
            if ind:
                pattern = re.compile(f'({re.escape(ind)})', re.IGNORECASE)
                result = pattern.sub(f'{Colors.RED}\\1{Colors.END}', result)
        return result
    # Handle OR logic
    elif '|' in indicator or re.search(r'\bOR\b', indicator, re.IGNORECASE):
        indicators = re.split(r'\s*\|\s*|\s+OR\s+', indicator, flags=re.IGNORECASE)
        result = message
        for ind in indicators:
            ind = ind.strip()
            if ind:
                pattern = re.compile(f'({re.escape(ind)})', re.IGNORECASE)
                result = pattern.sub(f'{Colors.RED}\\1{Colors.END}', result)
        return result
    else:
        # Single indicator
        pattern = re.compile(f'({re.escape(indicator)})', re.IGNORECASE)
        return pattern.sub(f'{Colors.RED}\\1{Colors.END}', message)

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

    # Handle AND logic (highest priority)
    if '&' in indicator or re.search(r'\bAND\b', indicator, re.IGNORECASE):
        # Split by & or AND (case insensitive) and clean up whitespace
        indicators = re.split(r'\s*&\s*|\s+AND\s+', indicator, flags=re.IGNORECASE)
        indicators = [ind.strip() for ind in indicators if ind.strip()]
        # Create AND logic: all terms must be present
        patterns = [re.compile(re.escape(ind), re.IGNORECASE) for ind in indicators]
        
        def matches_all(text):
            return all(pattern.search(text) for pattern in patterns)
        
        pattern_func = matches_all
        is_and_search = True
    # Handle OR logic  
    elif '|' in indicator or re.search(r'\bOR\b', indicator, re.IGNORECASE):
        # Split by | or OR (case insensitive) and clean up whitespace
        indicators = re.split(r'\s*\|\s*|\s+OR\s+', indicator, flags=re.IGNORECASE)
        indicators = [ind.strip() for ind in indicators if ind.strip()]
        # Create OR pattern: (word1|word2|word3)
        or_pattern = '|'.join(re.escape(ind) for ind in indicators)
        pattern = re.compile(f'({or_pattern})', re.IGNORECASE)
        pattern_func = pattern.search
        is_and_search = False
    else:
        # Single indicator search
        pattern = re.compile(indicator, re.IGNORECASE)
        pattern_func = pattern.search
        is_and_search = False

    end_offsets = consumer.end_offsets(topic_partitions)

    if latest or count == 1:
        matches = []
        for tp in topic_partitions:
            partition_matches = []
            current_offset = end_offsets[tp] - 1
            start_offset = max(end_offsets[tp] - scan_limit, 0)

            for offset in range(current_offset, start_offset - 1, -1):
                if offset < 0:
                    break
                consumer.seek(tp, offset)
                try:
                    message = next(consumer)
                    msg_value = message.value.decode('utf-8')
                    if pattern_func(msg_value):
                        partition_matches.append((message.timestamp, msg_value))
                        break
                except StopIteration:
                    break
            matches.extend(partition_matches)

        if matches:
            matches.sort(key=lambda x: x[0], reverse=True)
            consumer.close()
            return [matches[0][1]]
        else:
            consumer.close()
            return []

    else:
        matches_with_timestamps = []
        scanned = 0

        for tp in topic_partitions:
            start_offset = max(end_offsets[tp] - scan_limit, 0)
            consumer.seek(tp, start_offset)

        for message in consumer:
            scanned += 1
            msg_value = message.value.decode('utf-8')
            if pattern_func(msg_value):
                matches_with_timestamps.append((message.timestamp, msg_value))
                if count and len(matches_with_timestamps) >= count:
                    break
            if scanned >= scan_limit:
                break

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

def print_search_summary(topic, indicator, matched_count, scan_limit):
    """Print a colored summary of the search results"""
    print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
    print(f"{Colors.BOLD}Kafka Search Results{Colors.END}")
    print(f"{Colors.CYAN}{'='*60}{Colors.END}")
    print(f"Topic: {colorize_text(topic, Colors.YELLOW)}")
    print(f"Searching for: {colorize_text(indicator, Colors.RED)}")
    print(f"Matches found: {colorize_text(str(matched_count), Colors.GREEN)}")
    print(f"Scan limit: {colorize_text(str(scan_limit), Colors.BLUE)}")
    print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")

def print_message(message, indicator, index):
    """Print a message with highlighted indicators and formatting"""
    print(f"{Colors.BOLD}Message #{index + 1}:{Colors.END}")
    print(f"{Colors.CYAN}{'─' * 40}{Colors.END}")
    highlighted_message = highlight_indicator(message, indicator)
    print(highlighted_message)
    print(f"{Colors.CYAN}{'─' * 40}{Colors.END}\n")

def cli_interface():
    parser = argparse.ArgumentParser(description="Search Kafka messages from CLI")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--indicator", required=True, help="String to search for. Supports: 'word1 OR word2' or 'word1|word2' for OR logic, 'word1 AND word2' or 'word1&word2' for AND logic")
    parser.add_argument("--count", type=int, help="Number of matches to return")
    parser.add_argument("--latest", action="store_true", help="Return only latest match")
    parser.add_argument("--scan_limit", type=int, default=100, help="Number of messages to scan from latest")
    parser.add_argument("--output", help="File to dump results")
    parser.add_argument("--no-color", action="store_true", help="Disable colored output")

    args = parser.parse_args()

    # Disable colors if requested or if output is redirected
    if args.no_color or not sys.stdout.isatty():
        for attr in dir(Colors):
            if not attr.startswith('_'):
                setattr(Colors, attr, '')

    try:
        print(f"{Colors.YELLOW}Searching Kafka topic '{args.topic}' for '{args.indicator}'...{Colors.END}")
        
        messages = search_messages(
            args.topic,
            args.indicator,
            count=args.count,
            latest=args.latest,
            scan_limit=args.scan_limit
        )

        # Print search summary
        print_search_summary(args.topic, args.indicator, len(messages), args.scan_limit)

        if messages:
            if args.output:
                dump_messages_to_file(messages, args.output)
                print(f"{Colors.GREEN}Results saved to: {args.output}{Colors.END}\n")
            
            # Print each message with highlighting
            for i, message in enumerate(messages):
                print_message(message, args.indicator, i)
        else:
            print(f"{Colors.YELLOW}No messages found matching '{colorize_text(args.indicator, Colors.RED)}'{Colors.END}")
            
    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}")

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] != "runserver":
        cli_interface()
    else:
        print("Starting Flask API server at http://0.0.0.0:5000")
        app.run(host="0.0.0.0", port=5000)