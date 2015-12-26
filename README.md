# redis-pubsub
Python Redis pub-sub library

```python
import random
import time

def send():
	r = redis.Redis(host='127.0.0.1')
	p = Publisher(r, 'my publisher')
	for _ in range(1, 100):
		p.publish({'sleep': random.random()}, 'my topic', '1.0')

def receive():
	r = redis.Redis(host='127.0.0.1')
	filters = {
		'topic': lambda topic: topic == 'my topic',
		'version': lambda version: version >= '1.0',
	}
	Subscriber(r, 'my consumer').subscribe(handler, filters)

def handler(m):
	time.sleep(float(m['sleep']))
```
