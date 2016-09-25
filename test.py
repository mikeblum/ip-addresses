import csv
import random
import test_sql
import test_redis
import time

ipblock = lambda: random.randrange(1, 255)

def get_ipv4_addr():
    return '{}.{}.{}.{}'.format(ipblock(), ipblock(), ipblock(), ipblock())

with open('benchmarks.csv', 'w') as csvfile:
    fieldnames = ['ip_address', 'sql_time_secs', 'redis_time_secs']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    # writer.writerow({'first_name': 'Wonderful', 'last_name': 'Spam'})
    for n in range(0, 1000):
        ip = get_ipv4_addr()
        sql_start = time.time()
        sql_info = test_sql.get_info(ip)
        sql_time = time.time() - sql_start
        redis_start = time.time()
        redis_info = test_redis.get_info(ip)
        redis_time = time.time() - redis_start
        if sql_info and redis_info:
            writer.writerow({
                'ip_address': ip,
                'sql_time_secs': sql_time,
                'redis_time_secs': redis_time
            })