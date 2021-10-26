## MasTracking

Proyecto para la materia de la Universidad de Montevideo TIC V.


Quick Setup
-----------

1. Install the requirements in the virtualenv `pip install -r resources/requirements.txt`
2. Open a second terminal window and start a local Redis server (if you are on Linux or Mac, execute `run-redis.sh` to install and launch a private copy).
3. Start a Celery worker: `venv/bin/celery -A app.celery worker --loglevel=info` and beat `venv/bin/celery beat --app app.celery`
4. Start the Flask application: `venv/bin/python app.py`.
