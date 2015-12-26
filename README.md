# redis-pubsub
Python Redis pub-sub library

```python
import redis
import random
import time
from twiden.pubsub import Publisher, Subscriber

HOST = '127.0.0.1'

def send():
	r = redis.Redis(host=HOST)
	p = Publisher(r, 'my publisher')
	for _ in range(1, 100):
		p.publish({'sleep': random.random()}, 'my topic', '1.0')

def receive():
	r = redis.Redis(host=HOST)
	filters = {
		'topic': lambda topic: topic == 'my topic',
		'version': lambda version: version >= '1.0',
	}
	Subscriber(r, 'my consumer').subscribe(handler, filters)

def handler(m):
	zzz = m['sleep']
	print 'Sleeping', zzz
	time.sleep(zzz)
```
