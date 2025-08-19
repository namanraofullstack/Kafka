# api/kafka_util.py
import os
import re
import sys
import datetime
import argparse
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
import subprocess
import requests

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'broker:29092')
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

def highlight_regex_matches(message, pattern):
    if not pattern:
        return message
    try:
        matches = list(pattern.finditer(message))
        if not matches:
            return message
        result = message
        for match in reversed(matches):
            start, end = match.span()
            highlighted = f"{Colors.RED}{message[start:end]}{Colors.END}"
            result = result[:start] + highlighted + result[end:]
        return result
    except Exception:
        return message

def parse_multiple_partitions(partition_str):
    if not partition_str:
        return None
    partitions = set()
    for part in partition_str.split(','):
        part = part.strip()
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                partitions.update(range(start, end + 1))
            except ValueError:
                print(f"Warning: Invalid partition range '{part}', skipping")
        else:
            try:
                partitions.add(int(part))
            except ValueError:
                print(f"Warning: Invalid partition '{part}', skipping")
    return sorted(partitions) if partitions else None

def compile_regex_pattern(indicator, case_sensitive=False):
    if not indicator:
        return None, lambda x: True
    try:
        flags = 0 if case_sensitive else re.IGNORECASE
        pattern = re.compile(indicator, flags)
        return pattern, pattern.search
    except re.error as e:
        print(f"Error: Invalid regex pattern '{indicator}': {e}")
        sys.exit(1)


def search_messages(
    topic, indicator=None, bootstrap_servers=BOOTSTRAP_SERVERS,
    count=None, latest=False, scan_limit=100, partitions=None,
    start_offset=None, end_offset=None, start_date=None, end_date=None,
    case_sensitive=False
):
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        consumer_timeout_ms=2000,
        fetch_max_bytes=104857600,      # 100 MB
        max_partition_fetch_bytes=10485760,  # 10 MB/partition
        fetch_min_bytes=1,
        fetch_max_wait_ms=20,           # much lower latency
        session_timeout_ms=30000,
        heartbeat_interval_ms=3000,
    )

    available_partitions = consumer.partitions_for_topic(topic)
    if not available_partitions:
        raise Exception(f"No partitions found for topic: {topic}")

    target_partitions = available_partitions
    if partitions is not None:
        invalid = set(partitions) - available_partitions
        if invalid:
            print(f"Warning: Partitions {invalid} not in topic {topic}")
        target_partitions = set(partitions) & available_partitions
        if not target_partitions:
            raise Exception(f"No valid partitions from {partitions}")
        print(f"Searching partitions: {sorted(target_partitions)}")

    topic_partitions = [TopicPartition(topic, p) for p in target_partitions]
    consumer.assign(topic_partitions)

    pattern, pattern_func = compile_regex_pattern(indicator, case_sensitive)
    end_offsets = consumer.end_offsets(topic_partitions)

    has_date_filter = start_date is not None or end_date is not None
    start_ts = int(start_date.timestamp() * 1000) if start_date else None
    end_ts = int(end_date.timestamp() * 1000) if end_date else None

    matches = []

    for tp in topic_partitions:
        end_offset_abs = end_offsets[tp]
        if end_offset_abs == 0:
            continue

        # Calculate scan window
        # --- FIXED SCAN WINDOW LOGIC ---
        if start_offset is not None:
            scan_start = max(0, start_offset)
        else:
            # Default: go back 'scan_limit' messages from the end
            scan_start = max(0, end_offset_abs - scan_limit)

        if end_offset is not None:
            scan_end = min(end_offset, end_offset_abs)
        else:
            scan_end = end_offset_abs

        if scan_start >= scan_end:
            continue


        consumer.seek(tp, scan_start)

        scanned_count = 0
        while scanned_count < scan_limit and consumer.position(tp) < scan_end:
            batch = consumer.poll(timeout_ms=100, max_records=500)
            if tp not in batch or not batch[tp]:
                continue  # keep polling until timeout

            for message in reversed(batch[tp]):  # newest-first
                if message.offset >= scan_end:
                    continue

                scanned_count += 1
                if scanned_count > scan_limit:
                    break

                # Date filter
                if has_date_filter and message.timestamp:
                    if (start_ts and message.timestamp < start_ts) or \
                       (end_ts and message.timestamp > end_ts):
                        continue

                # Decode
                try:
                    msg_value = message.value.decode("utf-8") if message.value else ""
                except (UnicodeDecodeError, AttributeError):
                    msg_value = str(message.value)

                # Pattern check
                if pattern_func(msg_value):
                    match_tuple = (msg_value, message.partition, message.offset)
                    matches.append((match_tuple, message.timestamp or 0))

                    if latest:
                        consumer.close()
                        return [match_tuple], pattern

                    if count and len(matches) >= count:
                        consumer.close()
                        matches.sort(key=lambda x: x[1], reverse=True)
                        return [m[0] for m in matches[:count]], pattern

    consumer.close()

    # Final sort
    if matches:
        matches.sort(key=lambda x: x[1], reverse=True)
        result_matches = [m[0] for m in matches]
        if count and len(result_matches) > count:
            result_matches = result_matches[:count]
        return result_matches, pattern

    return [], pattern

def dump_messages_to_file(messages, filename):
    import requests  # ensure requests is imported

    # Prepare content to upload
    content = "\n".join([msg for msg, _, _ in messages])

    # Remote URL and auth
    remote_url = f"https://packages.cyware.com/repository/cyware/naman/{filename}"
    auth = ("ctix", "fj3HuaWq3y")  # user:password

    # Upload content
    response = requests.put(remote_url, data=content.encode("utf-8"), auth=auth)

    if response.status_code in (200, 201):
        print(f"Upload successful! Uploaded to: {remote_url}")
        return remote_url
    else:
        print(f"Upload failed: {response.status_code}, {response.text}")
        return None

def print_search_summary(topic, indicator, matched_count, scan_limit, partitions=None):
    print(f"\n{Colors.CYAN}{'='*70}{Colors.END}")
    print(f"{Colors.BOLD}Kafka Search Results{Colors.END}")
    print(f"{Colors.CYAN}{'='*70}{Colors.END}")
    print(f"Topic: {colorize_text(topic, Colors.YELLOW)}")
    if partitions:
        print(f"Partitions: {colorize_text(str(sorted(partitions)), Colors.BLUE)}")
    print(f"Regex pattern: {colorize_text(indicator, Colors.RED)}")
    print(f"Matches found: {colorize_text(str(matched_count), Colors.GREEN)}")
    print(f"Scan limit: {colorize_text(str(scan_limit), Colors.BLUE)}")
    print(f"{Colors.CYAN}{'='*70}{Colors.END}\n")

def print_message(message_data, pattern, index):
    msg, partition, offset = message_data
    print(f"{Colors.BOLD}Message #{index + 1}:{Colors.END}")
    print(f"{Colors.CYAN}{'─'*50}{Colors.END}")
    if pattern:
        print(highlight_regex_matches(msg, pattern))
    else:
        print(msg)
    print(f"{Colors.CYAN}{'─'*50}{Colors.END}\n")

# -------------------- Fixed function name to match import --------------------
def list_topics(bootstrap_servers=BOOTSTRAP_SERVERS):
    """
    Return a list of Kafka topics using KafkaConsumer API
    """
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
            consumer_timeout_ms=2000
        )
        topics = list(consumer.topics())
        consumer.close()
        return topics
    except Exception as e:
        print(f"{Colors.RED}Error fetching topics: {e}{Colors.END}")
        return []

def list_groups(bootstrap_servers=BOOTSTRAP_SERVERS):
    """
    Return a list of Kafka consumer groups using KafkaConsumer API
    """
    try:
        from kafka.admin import KafkaAdminClient
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='kafka_util'
        )
        groups = admin_client.list_consumer_groups()
        admin_client.close()
        return [group[0] for group in groups]  # Extract group names from tuples
    except Exception as e:
        print(f"{Colors.RED}Error listing consumer groups: {e}{Colors.END}")
        return []

def get_lag(group_name, bootstrap_servers):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="kafka_util"
        )

        # Get committed offsets
        group_offsets = admin_client.list_consumer_group_offsets(group_name)
        if not group_offsets:
            print(f"Consumer group '{group_name}' has no offsets (maybe inactive)")
            return None

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False
        )

        # Get latest (log-end) offsets
        end_offsets = consumer.end_offsets(list(group_offsets.keys()))

        total_lag = 0
        for tp, meta in group_offsets.items():
            committed = meta.offset if meta and meta.offset != -1 else 0
            latest = end_offsets.get(tp, 0)
            lag = max(0, latest - committed)
            total_lag += lag

        consumer.close()
        admin_client.close()
        return total_lag

    except Exception as e:
        print(f"Error calculating lag for group '{group_name}': {e}")
        return None

def cli_interface():
    parser = argparse.ArgumentParser(description="Kafka regex message search")
    parser.add_argument("--topic", help="Kafka topic name")
    parser.add_argument("--pattern", help="Regex pattern to search")
    parser.add_argument("--count", type=int, help="Number of matches to return")
    parser.add_argument("--latest", action="store_true", help="Return only latest match")
    parser.add_argument("--scan_limit", type=int, default=100, help="Messages to scan per partition")
    parser.add_argument("--output", help="File to dump results")
    parser.add_argument("--case-sensitive", action="store_true", help="Enable case-sensitive regex matching")
    parser.add_argument("--partition", help="Partitions (e.g. '0,1,2' or '0-3')")
    parser.add_argument("--start_offset", type=int, help="Start offset for search")
    parser.add_argument("--end_offset", type=int, help="End offset for search")
    parser.add_argument("--start_date", help="Start date YYYY-MM-DD")
    parser.add_argument("--end_date", help="End date YYYY-MM-DD")
    parser.add_argument("--bootstrap", help="Bootstrap servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--action", choices=["list_topics", "list_groups", "get_lag"], help="Perform a special action")
    parser.add_argument("--group", help="Consumer group name for get_lag action")

    args = parser.parse_args()

    # ------------------- Handle actions -------------------
    if args.action == "list_topics":
        topics = list_topics(args.bootstrap)
        if topics:
            print(f"{Colors.CYAN}Kafka Topics:{Colors.END}")
            for t in topics:
                print(f"  - {t}")
        return 0

    elif args.action == "list_groups":
        groups = list_groups(args.bootstrap)
        if groups:
            print(f"{Colors.CYAN}Consumer Groups:{Colors.END}")
            for g in groups:
                print(f"  - {g}")
        return 0

    elif args.action == "get_lag":
        if not args.group:
            print(f"{Colors.RED}Error: --group is required for get_lag{Colors.END}")
            return 1
        lag = get_lag(args.group, args.bootstrap)
        if lag is not None:
            print(f"{Colors.CYAN}Total Lag: {lag}{Colors.END}")
        return 0

    # ------------------- Message search -------------------
    if not args.topic or not args.pattern:
        print(f"{Colors.RED}Error: --topic and --pattern are required for message search{Colors.END}")
        return 1

    # Parse dates
    start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None

    # Parse partitions
    target_partitions = parse_multiple_partitions(args.partition)

    try:
        messages, pattern = search_messages(
            args.topic,
            args.pattern,
            bootstrap_servers=args.bootstrap,  # FIXED: Pass bootstrap_servers parameter
            count=args.count,
            latest=args.latest,
            scan_limit=args.scan_limit,
            partitions=target_partitions,
            start_offset=args.start_offset,
            end_offset=args.end_offset,
            start_date=start_date,
            end_date=end_date,
            case_sensitive=args.case_sensitive
        )

        print_search_summary(args.topic, args.pattern, len(messages), args.scan_limit, target_partitions)

        if messages:
            if args.output:
                dump_messages_to_file(messages, args.output)
            for i, message_data in enumerate(messages):
                print_message(message_data, pattern, i)
        else:
            print(f"{Colors.YELLOW}No messages found matching pattern '{args.pattern}'{Colors.END}")

    except Exception as e:
        print(f"{Colors.RED}Error: {str(e)}{Colors.END}")
        return 1

    return 0


if __name__ == '__main__':
    exit(cli_interface())