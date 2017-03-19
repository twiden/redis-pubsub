import uuid
import json
import socket
import sys
import traceback
import copy
from timeit import default_timer as timer
from datetime import datetime
from twiden import logging


CHANNEL = 'twiden'


class Subscriber(object):

    def __init__(self, redis, name):
        self.redis = redis
        self.logger = logging.getLogger(name)

    """Subscribe to channel
    @param: handler A callable
    @param: filters A list of _meta fields mapping to boolean functions
    """
    def subscribe(self, handler, filters):
        pubsub = self.redis.pubsub()
        pubsub.subscribe([CHANNEL])
        self.logger.info(what='subscribed')
        for message in pubsub.listen():
            data = {}

            try:
                data = json.loads(message['data'].decode('utf8'))
            except AttributeError:
                self.logger.warning(what='message_decode_error')
                continue
            except TypeError:
                self.logger.warning(what='json_parse_error')
                continue

            meta = data['_meta']

            try:
                if all(validator(meta.get(field)) for field, validator in filters.items()):
                    start = timer()
                    handler(copy.deepcopy(data))
                    end = timer()
                    self.logger.info(what='handler_ok', message=data, time=end - start)
            except Exception:
                etype, value, tb = sys.exc_info()
                self.logger.error(
                    what='handler_failed',
                    message=data,
                    traceback=''.join(traceback.format_exception(etype, value, tb)),
                )


class Publisher(object):

    def __init__(self, redis, name):
        self.redis = redis
        self.logger = logging.getLogger(name)

    """Publish a message to channel
    Keyword arguments:
    @param: data Dictionary message
    @param: topic Describe message
    @param: version Version of message format. For example '1.0'
    @param: causation_id Id of the message/event that preceeded this one
    @param: correlation_id Id of the first message/event in this chain of events

    """
    def publish(self, data, topic, version, causation_id=None, correlation_id=None):
        message = data.copy()
        message.update(
            {
                '_meta': {
                    'id': str(uuid.uuid1()),
                    'version': version,
                    'topic': topic,
                    'utc_timestamp': datetime.utcnow().isoformat(),
                    'causation_id': causation_id,
                    'correlation_id': correlation_id,
                    'hostname': socket.gethostname(),
                    'ip_address': socket.gethostbyname(socket.gethostname())
                }
            }
        )
        self.redis.publish(CHANNEL, json.dumps(message))
        self.logger.info(what='publish', message=message)
