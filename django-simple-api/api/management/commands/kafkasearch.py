from datetime import datetime
from django.core.management.base import BaseCommand
from api.kafka_util import (
    search_messages,
    dump_messages_to_file,
    print_search_summary,
    print_message,
    parse_multiple_partitions,
    list_topics,
    list_groups,  # Added new function import
    get_lag,      # Added new function import
    BOOTSTRAP_SERVERS,
)
from api.kafka_util import Colors

class Command(BaseCommand):
    help = "Kafka search CLI command"

    def add_arguments(self, parser):
        # Search arguments
        parser.add_argument("--topic", required=False, help="Kafka topic name")
        parser.add_argument("--pattern", required=False, help="Regex pattern to search")
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

        # New actions
        parser.add_argument("--list_topics", action="store_true", help="List all Kafka topics")
        parser.add_argument("--list_groups", action="store_true", help="List all Kafka consumer groups")
        parser.add_argument("--get_lag", help="Get total lag for a consumer group")

    def handle(self, *args, **options):
        bootstrap_servers = options.get("bootstrap", BOOTSTRAP_SERVERS)

        # ------------------- List topics -------------------
        if options.get("list_topics"):
            self.stdout.write("Fetching Kafka topics...")
            topics = list_topics(bootstrap_servers)
            if topics:
                self.stdout.write("Kafka Topics:")
                for t in topics:
                    self.stdout.write(f"  - {t}")
            else:
                self.stdout.write("No topics found.")
            return

        # ------------------- List consumer groups -------------------
        if options.get("list_groups"):
            self.stdout.write("Listing Kafka consumer groups...")
            groups = list_groups(bootstrap_servers)  # Use the new API-based function
            if groups:
                self.stdout.write("Consumer Groups:")
                for g in groups:
                    self.stdout.write(f"  - {g}")
            else:
                self.stdout.write("No consumer groups found.")
            return

        # ------------------- Get total lag -------------------
        group_name = options.get("get_lag")
        if group_name:
            self.stdout.write(f"Calculating total lag for group '{group_name}'...")
            lag = get_lag(group_name, bootstrap_servers)  # Use the new API-based function
            if lag is not None:
                self.stdout.write(f"Total Lag: {lag}")
            else:
                self.stdout.write(f"Could not calculate lag for group '{group_name}'")
            return

        # ------------------- Regular message search -------------------
        topic = options.get("topic")
        pattern = options.get("pattern")
        if not topic or not pattern:
            self.stderr.write("Error: --topic and --pattern are required for search")
            return

        count = options.get("count")
        latest = options.get("latest")
        scan_limit = options.get("scan_limit")
        output = options.get("output")
        case_sensitive = options.get("case_sensitive")
        partition = options.get("partition")
        start_offset = options.get("start_offset")
        end_offset = options.get("end_offset")
        start_date_str = options.get("start_date")
        end_date_str = options.get("end_date")

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None
        target_partitions = parse_multiple_partitions(partition)

        self.stdout.write(f"Searching Kafka topic '{topic}' for pattern '{pattern}'...")
        try:
            messages, pattern_obj = search_messages(
                topic,
                pattern,
                bootstrap_servers=bootstrap_servers,
                count=count,
                latest=latest,
                scan_limit=scan_limit,
                partitions=target_partitions,
                start_offset=start_offset,
                end_offset=end_offset,
                start_date=start_date,
                end_date=end_date,
                case_sensitive=case_sensitive,
            )

            print_search_summary(topic, pattern, len(messages), scan_limit, target_partitions)

            if messages:
                if output:
                    dump_messages_to_file(messages, output)
                    self.stdout.write(f"Results saved to: {output}")
                for i, message_data in enumerate(messages):
                    print_message(message_data, pattern_obj, i)
            else:
                self.stdout.write("No messages found matching your query.")
        except Exception as e:
            self.stderr.write(f"Error: {str(e)}")