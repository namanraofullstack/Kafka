# api/management/commands/kafkasearch.py
from django.core.management.base import BaseCommand
from api.kafka_util import search_messages, dump_messages_to_file, print_search_summary, print_message
from datetime import datetime

class Command(BaseCommand):
    help = "Kafka search CLI command"

    def add_arguments(self, parser):
        # Search arguments
        parser.add_argument("--topic", required=True, help="Kafka topic name")
        parser.add_argument("--indicator", required=True, help="Search string with OR/AND support")
        parser.add_argument("--count", type=int, help="Number of matches to return")
        parser.add_argument("--latest", action="store_true", help="Return only latest match")
        parser.add_argument("--scan-limit", type=int, default=100, help="Messages to scan from latest")
        parser.add_argument("--output", help="File to dump results")

        # New search filters
        parser.add_argument("--key", help="Message key to search for (exact match)")
        parser.add_argument("--partition", type=int, help="Partition number to search")
        parser.add_argument("--start_offset", type=int, help="Start offset for search")
        parser.add_argument("--end_offset", type=int, help="End offset for search")
        parser.add_argument("--start_date", help="Start date in YYYY-MM-DD format")
        parser.add_argument("--end_date", help="End date in YYYY-MM-DD format")

    def handle(self, *args, **options):
        topic = options['topic']
        indicator = options['indicator']
        count = options.get('count')
        latest = options.get('latest')
        scan_limit = options.get('scan_limit')
        output = options.get('output')

        # New filters
        key = options.get('key')
        partition = options.get('partition')
        start_offset = options.get('start_offset')
        end_offset = options.get('end_offset')
        start_date_str = options.get('start_date')
        end_date_str = options.get('end_date')
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None

        self.stdout.write(f"Searching Kafka topic '{topic}' for '{indicator}'...")

        messages = search_messages(
            topic,
            indicator,
            count=count,
            latest=latest,
            scan_limit=scan_limit,
            key=key,
            partition=partition,
            start_offset=start_offset,
            end_offset=end_offset,
            start_date=start_date,
            end_date=end_date
        )

        print_search_summary(topic, indicator, len(messages), scan_limit)

        if messages:
            if output:
                dump_messages_to_file(messages, output)
                self.stdout.write(f"Results saved to: {output}")
            for i, msg in enumerate(messages):
                print_message(msg, indicator, i)
        else:
            self.stdout.write("No messages found matching your query.")
