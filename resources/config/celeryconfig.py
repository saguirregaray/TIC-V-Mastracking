from datetime import timedelta


CELERY_ACCEPT_CONTENT = ['json', 'msgpack', 'yaml']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'

CELERYBEAT_SCHEDULE = {
    'sends-email-every-1-hour': {
        'task': 'tasks.alarm',
        'schedule': timedelta(minutes=1)
    },
}
