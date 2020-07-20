import multiprocessing
import os

import gunicorn

recommended_amount_of_workers = (multiprocessing.cpu_count() * 2) + 1

worker_class = 'gevent'
workers = os.environ.get('GUNICORN_WORKERS', recommended_amount_of_workers)
worker_connections = 1000

proc_name = 'data-store-service'
gunicorn.SERVER_SOFTWARE = proc_name

forwarded_allow_ips = '*'
x_forwarded_for_header = 'X-FORWARDED-FOR'
secure_scheme_headers = {
    'X-FORWARDED-PROTO': 'https',
}
timeout = 120
keepalive = 20
