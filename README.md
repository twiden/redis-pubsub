# redis-pubsub
Python Redis pub-sub library

def send():
	r = redis.Redis(host='pi3')
	p = Publisher(r, 'senter')
	import random
	for _ in range(1, 100):
		p.publish({'sleep': random.random()}, 'my topic', '1.0')

def receive():
	r = redis.Redis(host='pi3')
	filters = {
		'topic': lambda topic: topic == 'my topic',
		'version': lambda version: version >= '1.0',
	}
	Subscriber(r, 'pix').subscribe(h, filters)

def h(m):
	import time
	time.sleep(float(m['sleep']))
