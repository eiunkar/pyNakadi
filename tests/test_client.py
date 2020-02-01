from itertools import count

import json

from pyNakadi.client import NakadiClient, EndOfStreamException
from conftest import u8
import pytest
import time
import socket


def append_log(msg):
    with open('/tmp/logy', 'a') as f:
        f.write(f"{msg}\n")


def test_get_event_type(nakadi_url):
    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.get_event_type('pyNakadi_test_event_type_0')
    # print(resp)

    assert resp['name'] == 'pyNakadi_test_event_type_0'
    assert resp['owning_application'] == 'app1'
    assert resp['category'] == 'business'


def test_get_event_types(nakadi_url):
    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.get_event_types()
    # print(resp)
    event_type_names = [item['name'] for item in resp]
    assert 'pyNakadi_test_event_type_0' in event_type_names
    assert 'pyNakadi_test_event_type_1' in event_type_names


def test_delete_event_type(nakadi_url):
    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.delete_event_type('pyNakadi_test_event_type_2')
    assert resp == True
    resp = c.get_event_types()
    # print(resp)
    event_type_names = [item['name'] for item in resp]
    assert 'pyNakadi_test_event_type_0' in event_type_names
    assert 'pyNakadi_test_event_type_1' in event_type_names
    assert 'pyNakadi_test_event_type_2' not in event_type_names


def test_get_event_type_partitions(nakadi_url):
    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.get_event_type_partitions('pyNakadi_test_event_type_0')
    # print(resp)
    assert len(resp) == 2
    partition_infos = [
        {'oldest_available_offset': item['oldest_available_offset'],
         'newest_available_offset': item['newest_available_offset'], }
        for item in resp]
    assert {'oldest_available_offset': '001-0001-000000000000000000',
            'newest_available_offset': '001-0001-000000000000000004'} in partition_infos
    assert {'oldest_available_offset': '001-0001-000000000000000000',
            'newest_available_offset': '001-0001-000000000000000004'} in partition_infos


def test_get_event_type_partition(nakadi_url):
    c = NakadiClient('dummy_token', nakadi_url)
    resp_0 = c.get_event_type_partition('pyNakadi_test_event_type_0', '0')
    resp_1 = c.get_event_type_partition('pyNakadi_test_event_type_0', '1')
    # print(resp_0)
    # print(resp_1)
    resp = [resp_0, resp_1]
    partition_infos = [
        {'oldest_available_offset': item['oldest_available_offset'],
         'newest_available_offset': item['newest_available_offset'], }
        for item in resp]
    assert {'oldest_available_offset': '001-0001-000000000000000000',
            'newest_available_offset': '001-0001-000000000000000004'} in partition_infos
    assert {'oldest_available_offset': '001-0001-000000000000000000',
            'newest_available_offset': '001-0001-000000000000000004'} in partition_infos


def test_subscriptions(nakadi_url):
    c = NakadiClient('dummy_token', nakadi_url)
    subs = [c.create_subscription_v2(
        {
            'owning_application': 'app1',
            'event_types': [
                f'pyNakadi_test_event_type_{i}'
            ],
            'read_from': 'begin'
        }
    ) for i in range(0, 2)]
    status_codes_201 = [item[0] == 201 for item in subs]
    subs_ids = [item[1]['id'] for item in subs]
    assert all(status_codes_201)

    id = subs_ids[0]
    resp = c.get_subscription(id)
    # print(resp)
    assert resp['id'] == id
    assert resp['read_from'] == 'begin'
    assert resp['event_types'] == ['pyNakadi_test_event_type_0']

    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.get_subscription_stats(id, show_time_lag=True)
    # print(resp)
    assert resp == {'items': [{'event_type': 'pyNakadi_test_event_type_0',
                               'partitions': [{'partition': '0', 'state': 'unassigned', 'stream_id': ''},
                                              {'partition': '1', 'state': 'unassigned', 'stream_id': ''}]}]}

    subscriptions = []
    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.get_subscriptions(limit=1)
    # print(resp)
    subscriptions += resp['items']
    resp = c.get_next_subscriptions(resp)
    # print(resp)
    subscriptions += resp['items']
    resp = c.get_next_subscriptions(resp)
    # print(resp)
    subscriptions += resp['items']
    resp = c.get_next_subscriptions(resp)
    assert resp is None
    assert len(subscriptions) == 2
    # print(subscriptions)
    subscriptions_info = [
        {k: v for k, v in item.items() if k in ['owning_application', 'event_types', 'read_from', 'id']}
        for item in subscriptions
    ]
    # print(subs_ids)
    # print(subscriptions_info)
    assert {'owning_application': 'app1', 'event_types': ['pyNakadi_test_event_type_0'],
            'read_from': 'begin', 'id': subs_ids[0]} in subscriptions_info
    assert {'owning_application': 'app1', 'event_types': ['pyNakadi_test_event_type_1'],
            'read_from': 'begin', 'id': subs_ids[1]} in subscriptions_info

    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.get_subscriptions(owning_application='app1')
    # print(resp)
    subscriptions = resp['items']
    subscriptions_info = [
        {k: v for k, v in item.items() if k in ['owning_application', 'event_types', 'read_from', 'id']}
        for item in subscriptions
    ]
    assert {'owning_application': 'app1', 'event_types': ['pyNakadi_test_event_type_0'],
            'read_from': 'begin', 'id': subs_ids[0]} in subscriptions_info
    assert {'owning_application': 'app1', 'event_types': ['pyNakadi_test_event_type_1'],
            'read_from': 'begin', 'id': subs_ids[1]} in subscriptions_info

    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.get_subscriptions(owning_application='not_registered_app')
    # print(resp)
    assert resp == {'items': [], '_links': {}}

    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.get_subscriptions(event_type=['pyNakadi_test_event_type_0'])
    # print(resp)
    subscriptions = resp['items']
    subscriptions_info = [
        {k: v for k, v in item.items() if k in ['owning_application', 'event_types', 'read_from', 'id']}
        for item in subscriptions
    ]
    assert {'owning_application': 'app1', 'event_types': ['pyNakadi_test_event_type_0'],
            'read_from': 'begin', 'id': subs_ids[0]} in subscriptions_info

    c = NakadiClient('dummy_token', nakadi_url)
    resp = c.get_subscriptions(event_type=['not_registered_event_type'])
    # print(resp)
    assert resp == {'items': [], '_links': {}}

    c = NakadiClient('dummy_token', nakadi_url)
    c.delete_subscription(subs_ids[1])
    resp = c.get_subscriptions(owning_application='app1')
    # print(resp)
    subscriptions = resp['items']
    subscriptions_info = [
        {k: v for k, v in item.items() if k in ['owning_application', 'event_types', 'read_from', 'id']}
        for item in subscriptions
    ]
    assert {'owning_application': 'app1', 'event_types': ['pyNakadi_test_event_type_0'],
            'read_from': 'begin', 'id': subs_ids[0]} in subscriptions_info
    assert {'owning_application': 'app1', 'event_types': ['pyNakadi_test_event_type_1'],
            'read_from': 'begin', 'id': subs_ids[1]} not in subscriptions_info

    #
    # Read events
    #
    c = NakadiClient('dummy_token', nakadi_url)
    id = subs_ids[0]
    s = c.get_subscription_events_stream(id, batch_limit=1, stream_limit=10, batch_flush_timeout=5,
                                         max_uncommitted_events=100)
    batches = [json.loads(u8(batch)) for _, batch in zip(range(0, 15), s)]
    events = [event for batch in batches if 'events' in batch for event in batch['events']]
    s.close()
    events_info = [(item['field_1'], item['metadata']['eid']) for item in events]
    assert len(events_info) == 10
    # print(events_info)
    assert ('0', 'e6841941-caec-449d-afb3-43b071b595b4') in events_info
    assert ('1', '0b66f3de-9eba-4672-8c80-54325d78d828') in events_info
    assert ('2', '652d6ac3-6f31-4520-883d-b3658ccf37b3') in events_info
    assert ('3', 'efd6f303-ea11-499b-ae35-e37c96c1cf8e') in events_info
    assert ('4', '657c0b6f-06bf-454d-8104-e6531942742b') in events_info
    assert ('5', '7a0cbc35-ff8d-478e-90ff-070704a915e4') in events_info
    assert ('6', '31955e82-ecc5-449d-a14e-cc58276866c1') in events_info
    assert ('7', 'ff475e80-d35b-4641-8740-408a0557622c') in events_info
    assert ('8', '6bc083d9-e45c-4f3b-80fa-e140a5b8b6f8') in events_info
    assert ('9', 'c6e16ea0-5f66-4f5c-a98a-c0823004bbbb') in events_info

    # print(batches)
    all_cursors = [batch['cursor'] for batch in batches]
    req_cursors = [cursor for cursor in all_cursors if cursor['offset'] == '001-0001-000000000000000002']
    c = NakadiClient('dummy_token', nakadi_url)
    c.commit_subscription_cursors(id, s.stream_id, cursors=req_cursors)
    resp = c.get_subscription_cursors(id)
    cursor_info = [{k: v for k, v in cursor.items() if k in ['partition', 'offset', 'event_type']}
                   for cursor in resp['items']]
    assert {'partition': '1',
            'offset': '001-0001-000000000000000002',
            'event_type': 'pyNakadi_test_event_type_0', } in cursor_info
    assert {'partition': '0',
            'offset': '001-0001-000000000000000002',
            'event_type': 'pyNakadi_test_event_type_0', } in cursor_info

    resp = c.get_subscription_stats(id)
    assigment_info = [partition['state'] == 'assigned'
                      for stat in resp['items']
                      for partition in stat['partitions']]
    while any(assigment_info):
        time.sleep(10)
        resp = c.get_subscription_stats(id)
        assigment_info = [partition['state'] == 'assigned'
                          for stat in resp['items']
                          for partition in stat['partitions']]

    c = NakadiClient('dummy_token', nakadi_url)
    id = subs_ids[0]
    s = c.get_subscription_events_stream(id, batch_limit=1, stream_limit=10, batch_flush_timeout=5,
                                         max_uncommitted_events=100)
    batches = [json.loads(u8(batch)) for _, batch in zip(range(0, 15), s)]
    events = [event for batch in batches if 'events' in batch for event in batch['events']]
    s.close()
    events_info = [(item['field_1'], item['metadata']['eid']) for item in events]
    assert len(events_info) == 4
    assert ('6', '31955e82-ecc5-449d-a14e-cc58276866c1') in events_info
    assert ('7', 'ff475e80-d35b-4641-8740-408a0557622c') in events_info
    assert ('8', '6bc083d9-e45c-4f3b-80fa-e140a5b8b6f8') in events_info
    assert ('9', 'c6e16ea0-5f66-4f5c-a98a-c0823004bbbb') in events_info

    #
    # Test timeout
    #
    _, resp = c.create_subscription_v2(
        {
            'owning_application': 'app2',
            'event_types': [
                f'pyNakadi_test_event_type_0'
            ],
            'read_from': 'begin'
        }
    )
    id = resp['id']
    c = NakadiClient('dummy_token', nakadi_url)
    s = c.get_subscription_events_stream(id, batch_limit=1, stream_limit=10, batch_flush_timeout=5,
                                         stream_timeout=10)
    with pytest.raises(socket.timeout):
        batches = [json.loads(u8(batch)) for _, batch in zip(range(0, 15), s)]


def test_dummy():
    assert True
