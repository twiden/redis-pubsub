import uuid
import json
import socket
import sys
import traceback
import copy
from timeit import default_timer as timer
from datetime import datetime
from twiden import logging
import redis
import time

CHANNEL = 'twiden'


class PubsubException(Exception):
    pass


class Subscriber(object):

    def __init__(self, redis, name):
        self.redis = redis
        self.logger = logging.getLogger(name)

    def _subscribe(self, pubsub):
        max_iter = 5
        i = 0
        s = 0

        while i < max_iter:
            try:
                pubsub.subscribe([CHANNEL])
                self.logger.info(tag='redis.subscribe.ok', channel=CHANNEL, attempt=i)
                return
            except redis.exceptions.ConnectionError:
                s = s**2 or 2
                self.logger.warning(tag='redis.subscribe.attempt.failed', sleep=s, attempt=i, channel=CHANNEL)
                time.sleep(s)

        self.logger.error(tag='redis.subscribe.fail', attempt=i, channel=CHANNEL)
        raise PubsubException('Could not subscribe')

    """Subscribe to channel
    @param: handler A callable
    @param: filters A list of _meta fields mapping to boolean functions
    """
    def subscribe(self, handler, filters):
        pubsub = self.redis.pubsub()
        self._subscribe(pubsub)
        for message in pubsub.listen():
            data = {}
            try:
                data = json.loads(message['data'].decode('utf8'))
            except AttributeError:
                self.logger.warning(tag='redis.event.decode_error')
                continue
            except TypeError:
                self.logger.warning(tag='redis.event.parse_error')
                continue

            meta = data['_meta']

            try:
                self.logger.info(tag='redis.event.received', message=data)
                if all(validator(meta.get(field)) for field, validator in filters.items()):
                    start = timer()
                    handler(copy.deepcopy(data))
                    end = timer()
                    self.logger.info(tag='redis.event.handler.success', message=data, time=end - start)
                else:
                    self.logger.info(tag='redis.event.ignored', message=data)
            except Exception:
                etype, value, tb = sys.exc_info()
                self.logger.error(
                    tag='redis.event.handler.error',
                    message=data,
                    traceback=''.join(traceback.format_exception(etype, value, tb)),
                )


class Publisher(object):

    def __init__(self, redis, name):
        self.redis = redis
        self.logger = logging.getLogger(name)

    def _publish(self, message):
        max_iter = 5
        i = 0
        s = 0

        while i < max_iter:
            try:
                self.redis.publish(CHANNEL, json.dumps(message))
                self.logger.info(tag='redis.event.publish.success', message=message, attempt=i)
                return
            except redis.exceptions.ConnectionError:
                s = s**2 or 2
                self.logger.warning(
                    tag='redis.publish.attempt.failed',
                    sleep=s,
                    attempt=i,
                    channel=CHANNEL,
                    message=message
                )
                time.sleep(s)

        self.logger.error(tag='redis.publish.fail', attempt=i, channel=CHANNEL, message=message)
        raise PubsubException('Could not publish event')

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
        self._publish(message)
