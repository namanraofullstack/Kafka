import argparse
import os
import re
import sys
import datetime
from kafka import KafkaConsumer, TopicPartition
from kafka.partitioner.default import murmur2

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

def get_partition_for_key(key, partitions):
    """Match Kafka's default partitioner exactly (Murmur2)"""
    key_bytes = key.encode() if isinstance(key, str) else key
    return (murmur2(key_bytes) & 0x7fffffff) % len(partitions)

def search_messages(topic, indicator=None, key=None, bootstrap_servers=BOOTSTRAP_SERVERS,
                    count=None, latest=False, scan_limit=100,
                    partition=None, start_offset=None, end_offset=None, start_date=None, end_date=None):
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        consumer_timeout_ms=3000
    )

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise Exception(f"No partitions found for topic: {topic}")

    print(f"Partitions: {partitions}, Total: {len(partitions)}")

    # If searching by key, determine the partition
    if key is not None and partition is None:
        partition = get_partition_for_key(key, partitions)
        print(f"Key '{key}' maps to partition {partition}")

    # Restrict to a specific partition if given
    if partition is not None:
        if partition not in partitions:
            raise Exception(f"Partition {partition} not found in topic {topic}")
        partitions = {partition}

    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)

    # Compile pattern matching for payload
    if indicator:
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
    else:
        pattern_func = lambda x: True  # No indicator means match all

    end_offsets_dict = consumer.end_offsets(topic_partitions)

    for tp in topic_partitions:
        if start_offset is not None:
            consumer.seek(tp, start_offset)
        else:
            consumer.seek(tp, max(end_offsets_dict[tp] - scan_limit, 0))

    matches_with_timestamps = []
    scanned = 0

    for message in consumer:
        scanned += 1

        # Offset filter
        if start_offset is not None and message.offset < start_offset:
            continue
        if end_offset is not None and message.offset > end_offset:
            continue

        # Date filter
        msg_time = datetime.datetime.fromtimestamp(message.timestamp / 1000)
        if start_date and msg_time < start_date:
            continue
        if end_date and msg_time > end_date:
            continue

        # Key filter
        if key is not None:
            msg_key = message.key.decode('utf-8') if message.key else None
            if msg_key != key:
                continue

        # Payload filter
        msg_value = message.value.decode('utf-8')
        if pattern_func(msg_value):
            matches_with_timestamps.append((message.timestamp, msg_value))
            if count and len(matches_with_timestamps) >= count:
                break

        if scan_limit and scanned >= scan_limit and not (start_offset or end_offset):
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
    parser.add_argument("--key", help="Message key to search for (exact match)")
    parser.add_argument("--partition", type=int, help="Partition number to search")
    parser.add_argument("--start_offset", type=int, help="Start offset for search")
    parser.add_argument("--end_offset", type=int, help="End offset for search")
    parser.add_argument("--start_date", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", help="End date in YYYY-MM-DD format")

    args = parser.parse_args()

    if args.no_color or not sys.stdout.isatty():
        for attr in dir(Colors):
            if not attr.startswith('_'):
                setattr(Colors, attr, '')

    start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None

    try:
        print(f"{Colors.YELLOW}Searching Kafka topic '{args.topic}' for '{args.indicator}'...{Colors.END}")

        messages = search_messages(
            args.topic,
            args.indicator,
            key=args.key,
            count=args.count,
            latest=args.latest,
            scan_limit=args.scan_limit,
            partition=args.partition,
            start_offset=args.start_offset,
            end_offset=args.end_offset,
            start_date=start_date,
            end_date=end_date
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
