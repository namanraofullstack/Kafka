# api/management/commands/kafkasearch.py
from django.core.management.base import BaseCommand
from api.kafka_util import (
    search_messages,
    dump_messages_to_file,
    print_search_summary,
    print_message,
    parse_multiple_partitions,
)
from datetime import datetime


class Command(BaseCommand):
    help = "Kafka search CLI command"

    def add_arguments(self, parser):
        # Search arguments
        parser.add_argument("--topic", required=True, help="Kafka topic name")
        parser.add_argument("--pattern", required=True, help="Regex pattern to search")
        parser.add_argument("--count", type=int, help="Number of matches to return")
        parser.add_argument(
            "--latest", action="store_true", help="Return only latest match"
        )
        parser.add_argument(
            "--scan_limit",
            type=int,
            default=100,
            help="Messages to scan per partition",
        )
        parser.add_argument("--output", help="File to dump results")
        parser.add_argument(
            "--case-sensitive",
            action="store_true",
            help="Enable case-sensitive regex matching",
        )

        # Filters
        parser.add_argument(
            "--partition", help="Partitions (e.g. '0,1,2' or '0-3')"
        )
        parser.add_argument("--start_offset", type=int, help="Start offset for search")
        parser.add_argument("--end_offset", type=int, help="End offset for search")
        parser.add_argument("--start_date", help="Start date YYYY-MM-DD")
        parser.add_argument("--end_date", help="End date YYYY-MM-DD")

    def handle(self, *args, **options):
        topic = options["topic"]
        pattern = options["pattern"]
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

        # Parse dates
        start_date = (
            datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
        )
        end_date = (
            datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None
        )

        # Parse partitions using the same logic as standalone script
        target_partitions = parse_multiple_partitions(partition)

        self.stdout.write(f"Searching Kafka topic '{topic}' for pattern '{pattern}'...")
        
        

        try:
            start_time=datetime.now()
            messages, pattern_obj = search_messages(
                topic,
                pattern,
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
            end_time = datetime.now()
            elapsed = (end_time - start_time).total_seconds()  # convert timedelta to float seconds
            self.stdout.write(f"Search completed in {elapsed:.2f} seconds")

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
            return