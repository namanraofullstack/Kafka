import datetime
from django.http import JsonResponse
from django.views.decorators.http import require_GET
import time
import os

from .metrics_view import MESSAGES_SCANNED, SEARCH_ERRORS, SEARCH_REQUESTS_TOTAL
from api.kafka_util import (
    search_messages,
    parse_multiple_partitions,
    list_topics,
    list_groups,
    get_lag,
    dump_messages_to_file
)
from dotenv import load_dotenv

load_dotenv()  # loads .env into environment

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
UPLOAD_USER = os.getenv('UPLOAD_USER')
UPLOAD_PWD = os.getenv('UPLOAD_PWD')

@require_GET
def kafka_search(request):
    start_time = time.time()
    endpoint = '/api/search'
    method = request.method

    # Special actions
    action = request.GET.get('action')
    bootstrap_servers = request.GET.get('bootstrap', BOOTSTRAP_SERVERS)

    if action == "list_topics":
        try:
            topics = list_topics(bootstrap_servers)
            SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 200).inc()
            return JsonResponse({"action": "list_topics", "topics": topics, "count": len(topics)})
        except Exception as e:
            SEARCH_ERRORS.labels(topic="system", error_type=type(e).__name__).inc()
            SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 500).inc()
            return JsonResponse({"error": f"Failed to list topics: {str(e)}"}, status=500)

    elif action == "list_groups":
        try:
            groups = list_groups(bootstrap_servers)
            SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 200).inc()
            return JsonResponse({"action": "list_groups", "groups": groups, "count": len(groups)})
        except Exception as e:
            SEARCH_ERRORS.labels(topic="system", error_type=type(e).__name__).inc()
            SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 500).inc()
            return JsonResponse({"error": f"Failed to list groups: {str(e)}"}, status=500)

    elif action == "get_lag":
        group_name = request.GET.get('group')
        if not group_name:
            SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 400).inc()
            return JsonResponse({"error": "Missing required parameter 'group' for get_lag action"}, status=400)
        try:
            lag = get_lag(group_name, bootstrap_servers)
            if lag is not None:
                SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 200).inc()
                return JsonResponse({"action": "get_lag", "group": group_name, "total_lag": lag})
            else:
                SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 404).inc()
                return JsonResponse({"error": f"Consumer group '{group_name}' not found or has no offsets"}, status=404)
        except Exception as e:
            SEARCH_ERRORS.labels(topic="system", error_type=type(e).__name__).inc()
            SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 500).inc()
            return JsonResponse({"error": f"Failed to get lag for group '{group_name}': {str(e)}"}, status=500)

    # Regular search
    topic = request.GET.get('topic')
    indicator = request.GET.get('indicator') or request.GET.get('pattern')

    count = request.GET.get('count')
    latest = request.GET.get('latest', 'false').lower() == 'true'
    scan_limit = request.GET.get('scan_limit', 100)
    case_sensitive = request.GET.get('case_sensitive', 'false').lower() == 'true'
    partitions_str = request.GET.get('partitions') or request.GET.get('partition')
    start_offset = request.GET.get('start_offset')
    end_offset = request.GET.get('end_offset')
    start_date_str = request.GET.get('start_date')
    end_date_str = request.GET.get('end_date')
    output = request.GET.get('output')  # kept for API compatibility, no file saving

    try:
        count = int(count) if count else None
        scan_limit = int(scan_limit)
        start_offset = int(start_offset) if start_offset else None
        end_offset = int(end_offset) if end_offset else None
    except ValueError:
        SEARCH_ERRORS.labels(topic=topic or "unknown", error_type="invalid_params").inc()
        SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 400).inc()
        return JsonResponse({"error": "count, scan_limit, start_offset, end_offset must be integers"}, status=400)

    target_partitions = None
    if partitions_str:
        try:
            target_partitions = parse_multiple_partitions(partitions_str)
        except Exception as e:
            SEARCH_ERRORS.labels(topic=topic or "unknown", error_type="invalid_partitions").inc()
            SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 400).inc()
            return JsonResponse({"error": f"Invalid partitions format: {str(e)}"}, status=400)

    start_date = end_date = None
    try:
        if start_date_str:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
        if end_date_str:
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        SEARCH_ERRORS.labels(topic=topic or "unknown", error_type="invalid_date_format").inc()
        SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 400).inc()
        return JsonResponse({"error": "Dates must be in YYYY-MM-DD format"}, status=400)

    if not topic or not indicator:
        SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 400).inc()
        return JsonResponse({"error": "Missing required parameters 'topic' and 'indicator'"}, status=400)

    try:
        matches, pattern = search_messages(
            topic=topic,
            indicator=indicator,
            bootstrap_servers=bootstrap_servers,
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
        MESSAGES_SCANNED.labels(topic=topic).inc(scan_limit)
    except Exception as e:
        SEARCH_ERRORS.labels(topic=topic, error_type=type(e).__name__).inc()
        SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 500).inc()
        return JsonResponse({"error": f"Kafka error: {str(e)}"}, status=500)

    # No local file saving — only keep remote dump if available
    output_url = None
    if output:
        try:
            output_url = dump_messages_to_file(matches, output)
        except Exception:
            output_url = None  # Fail silently if dump is not supported

    execution_time = round(time.time() - start_time, 3)
    SEARCH_REQUESTS_TOTAL.labels(method, endpoint, 200).inc()

    response_data = {
        "matched_count": len(matches),
        "messages": [{"message": m, "partition": p, "offset": o} for m, p, o in matches],
        "search_params": {
            "topic": topic,
            "indicator": indicator,
            "bootstrap_servers": bootstrap_servers,
            "partitions": sorted(target_partitions) if target_partitions else "all",
            "count": count,
            "latest": latest,
            "scan_limit": scan_limit,
            "case_sensitive": case_sensitive,
            "start_offset": start_offset,
            "end_offset": end_offset,
            "start_date": start_date_str,
            "end_date": end_date_str
        },
        "execution_time_seconds": execution_time
    }

    if output_url:
        response_data["output"] = {
            "filename": output,
            "url": output_url,
            "message_count": len(matches)
        }

    return JsonResponse(response_data)
