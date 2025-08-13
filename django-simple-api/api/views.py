import os
import re
from kafka import KafkaConsumer, TopicPartition
from django.http import JsonResponse
from django.views.decorators.http import require_GET
import time
from .metrics_view import MESSAGES_SCANNED, SEARCH_ERRORS, SEARCH_REQUESTS_TOTAL
from django.utils import timezone

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
        SEARCH_ERRORS.labels(topic=topic, error_type='no_partitions').inc()
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

    scanned = 0
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
                    scanned += 1
                    msg_value = message.value.decode('utf-8')
                    if pattern_func(msg_value):
                        partition_matches.append((message.timestamp, msg_value))
                        break
                except StopIteration:
                    break
            matches.extend(partition_matches)
        MESSAGES_SCANNED.labels(topic=topic).inc(scanned)
        consumer.close()

        if matches:
            matches.sort(key=lambda x: x[0], reverse=True)
            return [matches[0][1]]
        return []
    else:
        matches_with_timestamps = []

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

        MESSAGES_SCANNED.labels(topic=topic).inc(scanned)
        consumer.close()

        matches_with_timestamps.sort(key=lambda x: x[0], reverse=True)
        return [msg for _, msg in matches_with_timestamps]


@require_GET
def kafka_search(request):
    start_time = time.time()
    endpoint = '/api/search'
    method = request.method

    topic = request.GET.get('topic')
    indicator = request.GET.get('indicator')
    count = request.GET.get('count')
    latest = request.GET.get('latest', 'false').lower() == 'true'
    scan_limit = request.GET.get('scan_limit', 100)
    output = request.GET.get('output')

    try:
        count = int(count) if count else None
        scan_limit = int(scan_limit)
    except ValueError:
        SEARCH_ERRORS.labels(topic=topic or "unknown", error_type="invalid_params").inc()
        SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 400).inc()
        return JsonResponse({"error": "count and scan_limit must be integers"}, status=400)

    if not topic or not indicator:
        SEARCH_ERRORS.labels(topic=topic or "unknown", error_type="missing_params").inc()
        SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 400).inc()
        return JsonResponse({"error": "Missing required parameters 'topic' and 'indicator'"}, status=400)

    try:
        messages = search_messages(topic, indicator, count=count, latest=latest, scan_limit=scan_limit)
    except Exception as e:
        SEARCH_ERRORS.labels(topic=topic, error_type=type(e).__name__).inc()
        SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 500).inc()
        return JsonResponse({"error": f"Kafka error: {str(e)}"}, status=500)

    if output:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        path = os.path.join(OUTPUT_DIR, output)
        with open(path, 'w', encoding='utf-8') as f:
            for m in messages:
                f.write(m + '\n')

    SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 200).inc()

    return JsonResponse({
        "matched_count": len(messages),
        "messages": messages
    })