import argparse
import os
import re
import sys
from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', './output')

class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'

def colorize_text(text, color):
    return f"{color}{text}{Colors.END}"

def highlight_indicator(message, indicator):
    if '&' in indicator or re.search(r'\bAND\b', indicator, re.IGNORECASE):
        indicators = re.split(r'\s*&\s*|\s+AND\s+', indicator, flags=re.IGNORECASE)
        result = message
        for ind in indicators:
            ind = ind.strip()
            if ind:
                pattern = re.compile(f'({re.escape(ind)})', re.IGNORECASE)
                result = pattern.sub(f'{Colors.RED}\\1{Colors.END}', result)
        return result
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

    if '&' in indicator or re.search(r'\bAND\b', indicator, re.IGNORECASE):
        indicators = re.split(r'\s*&\s*|\s+AND\s+', indicator, flags=re.IGNORECASE)
        indicators = [ind.strip() for ind in indicators if ind.strip()]
        patterns = [re.compile(re.escape(ind), re.IGNORECASE) for ind in indicators]
        def matches_all(text):
            return all(pattern.search(text) for pattern in patterns)
        pattern_func = matches_all
    elif '|' in indicator or re.search(r'\bOR\b', indicator, re.IGNORECASE):
        indicators = re.split(r'\s*\|\s*|\s+OR\s+', indicator, flags=re.IGNORECASE)
        indicators = [ind.strip() for ind in indicators if ind.strip()]
        or_pattern = '|'.join(re.escape(ind) for ind in indicators)
        pattern = re.compile(f'({or_pattern})', re.IGNORECASE)
        pattern_func = pattern.search
    else:
        pattern = re.compile(indicator, re.IGNORECASE)
        pattern_func = pattern.search

    end_offsets = consumer.end_offsets(topic_partitions)

    if latest or count == 1:
        matches = []
        for tp in topic_partitions:
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
                        matches.append((message.timestamp, msg_value))
                        break
                except StopIteration:
                    break
        consumer.close()
        if matches:
            matches.sort(key=lambda x: x[0], reverse=True)
            return [matches[0][1]]
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

        consumer.close()
        matches_with_timestamps.sort(key=lambda x: x[0], reverse=True)
        return [msg for _, msg in matches_with_timestamps]

def dump_messages_to_file(messages, filename):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    print(f"Dumping {len(messages)} messages to file: {path}")
    with open(path, 'w', encoding='utf-8') as f:
        for m in messages:
            f.write(m + '\n')

def print_search_summary(topic, indicator, matched_count, scan_limit):
    print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
    print(f"{Colors.BOLD}Kafka Search Results{Colors.END}")
    print(f"{Colors.CYAN}{'='*60}{Colors.END}")
    print(f"Topic: {colorize_text(topic, Colors.YELLOW)}")
    print(f"Searching for: {colorize_text(indicator, Colors.RED)}")
    print(f"Matches found: {colorize_text(str(matched_count), Colors.GREEN)}")
    print(f"Scan limit: {colorize_text(str(scan_limit), Colors.BLUE)}")
    print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")

def print_message(message, indicator, index):
    print(f"{Colors.BOLD}Message #{index + 1}:{Colors.END}")
    print(f"{Colors.CYAN}{'─' * 40}{Colors.END}")
    print(highlight_indicator(message, indicator))
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

        print_search_summary(args.topic, args.indicator, len(messages), args.scan_limit)

        if messages:
            if args.output:
                dump_messages_to_file(messages, args.output)
                print(f"{Colors.GREEN}Results saved to: {args.output}{Colors.END}\n")
            for i, message in enumerate(messages):
                print_message(message, args.indicator, i)
        else:
            print(f"{Colors.YELLOW}No messages found matching '{colorize_text(args.indicator, Colors.RED)}'{Colors.END}")

    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}")

if __name__ == '__main__':
    cli_interface()
