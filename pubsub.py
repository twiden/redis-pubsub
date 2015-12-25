import redis
import uuid
import json
from timeit import defaulttimer
from datetime import datetime


CHANNEL = 'twiden'


class Subscriber(object):

    def __init__(self, redis, name):
        self.redis = redis
        self.logger = getLogger(name)

    def subscribe(self, handler, filters):
        pubsub = self.redis.pubsub()
        pubsub.subscribe([CHANNEL])
        self.logger.info({'what': 'waiting for messages'})
        for message in pubsub.listen():
            try:
                data = json.loads(message['data'])
                meta = data['_meta']
                if all(validator(meta.get(field)) for field, validator in filters.items()):
                    start = timer()
                    handler(data)
                    end = timer()
                    self.logger.info({'what': 'handler ok', 'message': data, 'time': end - start})
            except Exception as ex:
                self.logger.error({'what': 'handler failed', 'message': data, 'exception': str(ex)})


class Publisher(object):

    def __init__(self, redis, name):
        self.redis = redis
        self.logger = getLogger(name)

    def publish(self, data, topic, version, causation_id=None, correlation_id=None):
        message = data.copy()
        message.update({'_meta': {
            'id': str(uuid1()),
            'version': version,
            'topic': topic,
            'utc_timestamp': datetime.utcnow().isoformat()
            'causation_id': causation_id,
            'correlation_id': correlation_id
            }
        })
        self.redis.publish(CHANNEL, json.dumps(message))
        self.logger.info({'what': 'message published', 'message': message})
