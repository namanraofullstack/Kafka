import os
import datetime
from django.http import JsonResponse
from django.views.decorators.http import require_GET
import time

from .metrics_view import MESSAGES_SCANNED, SEARCH_ERRORS, SEARCH_REQUESTS_TOTAL
from api.kafka_util import search_messages, parse_multiple_partitions  # Import everything from your utility

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', './output')


@require_GET
def kafka_search(request):
    start_time = time.time()
    endpoint = '/api/search'
    method = request.method

    # Required params
    topic = request.GET.get('topic')
    indicator = request.GET.get('indicator') or request.GET.get('pattern')

    # Optional params
    count = request.GET.get('count')
    latest = request.GET.get('latest', 'false').lower() == 'true'
    scan_limit = request.GET.get('scan_limit', 100)
    case_sensitive = request.GET.get('case_sensitive', 'false').lower() == 'true'
    partitions_str = request.GET.get('partitions')
    start_offset = request.GET.get('start_offset')
    end_offset = request.GET.get('end_offset')
    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')
    output = request.GET.get('output')

    # Convert numeric params
    try:
        count = int(count) if count else None
        scan_limit = int(scan_limit)
        start_offset = int(start_offset) if start_offset else None
        end_offset = int(end_offset) if end_offset else None
    except ValueError:
        SEARCH_ERRORS.labels(topic=topic or "unknown", error_type="invalid_params").inc()
        SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 400).inc()
        return JsonResponse({"error": "count, scan_limit, start_offset, end_offset must be integers"}, status=400)

    # Parse partitions
    target_partitions = None
    if partitions_str:
        try:
            target_partitions = parse_multiple_partitions(partitions_str)
        except Exception as e:
            return JsonResponse({"error": f"Invalid partitions format: {str(e)}"}, status=400)

    # Parse dates
    start_date = end_date = None
    try:
        if start_date_str:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
        if end_date_str:
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        return JsonResponse({"error": "Dates must be in YYYY-MM-DD format"}, status=400)

    if not topic or not indicator:
        return JsonResponse({"error": "Missing required parameters 'topic' and 'indicator'"}, status=400)

    # Call Kafka search
    try:
        matches, _ = search_messages(
            topic=topic,
            indicator=indicator,
            count=count,
            latest=latest,
            scan_limit=scan_limit,
            partitions=target_partitions,
            start_offset=start_offset,
            end_offset=end_offset,
            start_date=start_date,
            end_date=end_date,
            case_sensitive=case_sensitive
        )
    except Exception as e:
        SEARCH_ERRORS.labels(topic=topic, error_type=type(e).__name__).inc()
        SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 500).inc()
        return JsonResponse({"error": f"Kafka error: {str(e)}"}, status=500)

    # Save output if requested
    if output:
        try:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            path = os.path.join(OUTPUT_DIR, output)
            with open(path, 'w', encoding='utf-8') as f:
                for msg, partition, offset in matches:
                    f.write(f"# Partition: {partition}, Offset: {offset}\n")
                    f.write(msg + '\n\n')
        except Exception as e:
            return JsonResponse({"error": f"Failed to write output file: {str(e)}"}, status=500)

    SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 200).inc()

    return JsonResponse({
        "matched_count": len(matches),
        "messages": [{"message": m, "partition": p, "offset": o} for m, p, o in matches],
        "search_params": {
            "topic": topic,
            "indicator": indicator,
            "partitions": sorted(target_partitions) if target_partitions else "all",
            "count": count,
            "latest": latest,
            "scan_limit": scan_limit,
            "case_sensitive": case_sensitive
        }
    })
