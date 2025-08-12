# api/management/commands/kafkasearch.py
from django.core.management.base import BaseCommand
from api.kafka_util import search_messages, dump_messages_to_file, print_search_summary, print_message

class Command(BaseCommand):
    help = "Kafka search CLI command"

    def add_arguments(self, parser):
        parser.add_argument("--topic", required=True, help="Kafka topic name")
        parser.add_argument("--indicator", required=True, help="Search string with OR/AND support")
        parser.add_argument("--count", type=int, help="Number of matches to return")
        parser.add_argument("--latest", action="store_true", help="Return only latest match")
        parser.add_argument("--scan-limit", type=int, default=100, help="Messages to scan from latest")
        parser.add_argument("--output", help="File to dump results")
        # NO --no-color here because Django already has it internally

    def handle(self, *args, **options):
        topic = options['topic']
        indicator = options['indicator']
        count = options.get('count')
        latest = options.get('latest')
        scan_limit = options.get('scan_limit')
        output = options.get('output')
        
        self.stdout.write(f"Searching Kafka topic '{topic}' for '{indicator}'...")

        messages = search_messages(
            topic,
            indicator,
            count=count,
            latest=latest,
            scan_limit=scan_limit
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
