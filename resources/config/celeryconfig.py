from datetime import timedelta


CELERY_ACCEPT_CONTENT = ['json', 'msgpack', 'yaml']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'

CELERYBEAT_SCHEDULE = {
    'monitor-every-1-hour': {
        'task': 'tasks.monitor',
        'schedule': timedelta(hours=1)
    },
}
