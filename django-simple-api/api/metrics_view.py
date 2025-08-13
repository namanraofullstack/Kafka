from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from django.http import HttpResponse

# Existing metrics
MESSAGES_SCANNED = Counter(
    'kafka_search_messages_scanned_total',
    'Total number of Kafka messages scanned',
    ['topic']
)

SEARCH_ERRORS = Counter(
    'kafka_search_errors_total',
    'Total number of errors during Kafka search',
    ['topic', 'error_type']
)

# New metric: count total requests made to search API
SEARCH_REQUESTS_TOTAL = Counter(
    'kafka_search_requests_total',
    'Total number of Kafka search API requests',
    ['method', 'endpoint', 'status_code']
)

def metrics(request):
    data = generate_latest()
    return HttpResponse(data, content_type=CONTENT_TYPE_LATEST)
