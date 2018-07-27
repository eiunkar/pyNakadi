# pyNakadi
Python client for Nakadi

## Installation

You can install pyNakadi via pip.

`pip install pyNakadi`

You can find pyPI project here:

https://pypi.python.org/pypi/pyNakadi/


## Examples
### Post event
``` python
from pyNakadi import NakadiClient, NakadiException
import pytz
import uuid
from datetime import datetime

token = '<your auth token here>'
url = '<nakadi url>'

time = datetime.utcnow().replace(tzinfo=pytz.UTC)
eid = uuid.uuid4()

event = {
    "metadata": {
        "eid": str(eid),
        "occurred_at": time.isoformat()
    },
    "property1": "value1",
    "property2": "value2"
}
event_type = '<your event type>'


client = NakadiClient(token, url)

try:
    client.post_events(event_type, [event])
except NakadiException as ex:
    print(f'NakadiException[{ex.code}]: {ex.msg}')
```

###  Read event
``` python
import pytz
import json
from pyNakadi import NakadiClient, NakadiException, NakadiStream
import logging
logger = logging.getLogger("NAKADI")

token = '<your auth token>'
host = '<nakadi host>'
subscription_id = '<subscription id>'


def get_subscription(token, host, subscription_id, limit):
    try:
        client = NakadiClient(token, host)
        return client.get_subscription_events_stream(subscription_id, batch_limit=limit, stream_limit=limit)
    except NakadiException as ex:
        logger.exception(ex.msg, exc_info=ex)
        raise ex


def get_batch(token, host, subscription_id, limit):
    try:
        subscription = get_subscription(token, host, subscription_id, limit)
        return json.loads(subscription.__next__()), subscription.stream_id
    except Exception as ex:
        logger.exception(
            'Exception while fetching events from Nakadi', exc_info=ex)
        raise ex
    finally:
        subscription.close()
        logger.exception(subscription.closed())


def commit_cursors(token, host, subscription_id, stream_id, cursors):
    try:
        client = NakadiClient(token, host)
        client.commit_subscription_cursors(subscription_id, stream_id, cursors)
    except NakadiException as ex:
        logger.exception(ex.msg, exc_info=ex)
        raise ex


batch, stream_id = get_batch(token, host, subscription_id, 10)
cursor = batch.get('cursor')
events = batch.get('events')
try:
    for event in events:
        # process the event
        pass
    commit_cursors(token, host, subscription_id, stream_id, [cursor])
except Exception as ex:
    logger.exception(
        'Exception while processing Nakadi events', exc_info=ex)
    raise ex
```
