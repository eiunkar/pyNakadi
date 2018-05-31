# pyNakadi
Python client for Nakadi

## Installation

You can install pyNakadi via pip.

`pip install pyNakadi`

You can find pyPI project here:

https://pypi.python.org/pypi/pyNakadi/


## Example
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
