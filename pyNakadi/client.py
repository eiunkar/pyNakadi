import json
import socket
import uuid
from functools import reduce

import requests
import copy


class NakadiException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __str__(self):
        return f"NakadiException(code={self.code}, msg={self.msg})"


class EndOfStreamException(Exception):
    pass


class EndOfStreamException0(Exception):
    pass


class NakadiStream():
    """
    Iterator that generates batches. This stream is either created by a
    get_subscription_events_stream method or get_event_type_events_stream
    method.
    """
    BUFFER_SIZE = 64 * 1024

    def __init__(self, response):
        self.response = response
        self.sock = self.response.raw.connection.sock

        self.raw_buffer = b''
        self.buffer = b''
        self.current_batch = None
        self.__it = response.iter_lines(chunk_size=1)

        if hasattr(self.sock, 'socket'):
            self.sock.socket.settimeout(30)
            self.sock.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        else:
            self.sock.settimeout(30)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if 'X-Nakadi-StreamId' in self.response.headers:
            self.stream_id = self.response.headers['X-Nakadi-StreamId']
        else:
            self.stream_id = str(uuid.uuid4())

    def read_buffer(self):
        buffer = self.sock.recv(self.BUFFER_SIZE)
        return buffer

    def read_chunk(self):

        if b'\r\n' not in self.raw_buffer:

            while self.raw_buffer[-2:] != b'\r\n':
                received_byte = self.sock.recv(1)
                if received_byte == b'':
                    raise EndOfStreamException
                self.raw_buffer += received_byte
            size_b = self.raw_buffer[:-2]
            self.raw_buffer = b''
        else:
            size_b, self.raw_buffer = self.raw_buffer.split(b'\r\n', 1)
        size = int(size_b, 16) + 2

        data_read_arr = list()
        remaining = size
        data_read = self.raw_buffer
        data_read_arr.append(data_read)
        remaining -= len(data_read)
        while remaining > 0:
            data_read = self.sock.recv(self.BUFFER_SIZE)
            if data_read == b'':
                raise EndOfStreamException
            data_read_arr.append(data_read)
            remaining -= len(data_read)
        if remaining != 0:
            self.raw_buffer = data_read_arr[-1][remaining:]
        else:
            self.raw_buffer = b''

        if len(data_read) + remaining == 1:
            data_read_arr[-2] = data_read_arr[-2][:-1]
            data_read_arr.pop(-1)
        else:
            data_read_arr[-1] = data_read_arr[-1][:remaining - 2]
        data_b = b''.join(data_read_arr)

        if size == 0:
            raise EndOfStreamException0

        return data_b

    def __iter__(self):
        return self

    def __next__(self):
        data_read_arr = list()
        data_read = self.buffer
        data_read_arr.append(data_read)
        while b'\n' not in data_read:
            data_read = self.read_chunk()
            data_read_arr.append(data_read)
        data_read_arr[-1], self.buffer = data_read_arr[-1].split(b'\n', 1)
        data_b = b''.join(data_read_arr)
        self.current_batch = data_b
        return self.current_batch

    def get_stream_id(self):
        """
        :return: X-Nakadi-StreamId
        """
        return self.stream_id

    def close(self):
        """
        Closes network stream.
        :return:
        """
        self.response.raw.close()

    def closed(self):
        """
        Flag if network stream is closed or not.
        :return:
        """
        return self.response.raw.closed


class NakadiClient:
    def __init__(self, token, nakadi_url):
        """
        Initiates a Nakadi client using the token and aiming for url
        :param token: token string to be used
        :param nakadi_url: url for nakadi server
        """
        self.token = token
        self.nakadi_url = nakadi_url
        self.session = self.__create_session(token)

    def __create_session(self, token):
        result = requests.Session()
        result.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        })
        return result

    @classmethod
    def assert_it(cls, condition, exception):
        if not condition:
            raise exception

    def get_metrics(self):
        """
        GET /metrics
        :return:
        """
        page = f"{self.nakadi_url}/metrics"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_metrics. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def get_event_types(self):
        """
        GET /event-types
        :return:
        """
        page = f"{self.nakadi_url}/event-types"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_types. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def create_event_type(self, event_type_data_map):
        """
        POST /event-types
        :param event_type_data_map:
        :return:
        """
        page = f"{self.nakadi_url}/event-types"
        response = self.session.post(page, json=event_type_data_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [201]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during create_event_type. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        return True

    def get_event_type(self, event_type_name):
        """
        GET /event-types/{name}
        :param event_type_name:
        :return:
        """
        page = f"{self.nakadi_url}/event-types/{event_type_name}"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def update_event_type(self, event_type_name, event_type_data_map):
        """
        PUT /event-types/{name}
        :param event_type_name:
        :param event_type_data_map:
        :return:
        """
        page = f"{self.nakadi_url}/event-types/{event_type_name}"
        response = self.session.put(page, json=event_type_data_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during update_event_type. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        return True

    def delete_event_type(self, event_type_name):
        """
        DELETE /event-types/{name}
        :param event_type_name:
        :return:
        """
        page = f"{self.nakadi_url}/event-types/{event_type_name}"
        response = self.session.delete(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during delete_event_type. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        return True

    def get_event_type_cursor_distances(self, event_type_name, query_map):
        """
        POST /event-types/{name}/cursor-distances
        :param event_type_name:
        :param query_map:
        :return:
        """
        page = f"{self.nakadi_url}/event-types/{event_type_name}/cursor-distances"
        response = self.session.post(page, json=query_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type_cursor_distances. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def get_event_type_cursor_lag(self, event_type_name, cursors_map):
        """
        POST /event-types/{name}/cursors-lag
        :param event_type_name:
        :param cursors_map:
        :return:
        """
        page = f"{self.nakadi_url}/event-types/{event_type_name}/cursor-lag"
        response = self.session.post(page, json=cursors_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type_cursor_lag. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def post_events(self, event_type_name, events):
        """
        POST /event-types/{name}/events
        :param event_type_name:
        :param events:
        :return:
        """
        page = f"{self.nakadi_url}/event-types/{event_type_name}/events"
        response = self.session.post(page, json=events)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during post_events. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        return True

    def get_event_type_events_stream(self,
                                     event_name,
                                     batch_limit=1,
                                     stream_limit=0,
                                     batch_flush_timeout=30,
                                     stream_timeout=0,
                                     stream_keep_alive_limit=0,
                                     cursors=None):
        """
        GET /event-types/{name}/events
        :param event_name:
        :param batch_limit:
        :param stream_limit:
        :param batch_flush_timeout:
        :param stream_timeout:
        :param stream_keep_alive_limit:
        :param cursors:
        :return: NakadiStream
        """
        headers = copy.copy(self.session.headers)
        if cursors is not None:
            headers['X-nakadi-cursors'] = json.dumps(cursors)
        page = f"{self.nakadi_url}/event-types/{''}/events"
        query_str = ''
        if batch_limit is not None:
            query_str += f'&batch_limit={batch_limit}'
        if stream_limit is not None:
            query_str += f'&stream_limit={stream_limit}'
        if batch_flush_timeout is not None:
            query_str += f'&batch_flush_timeout={batch_flush_timeout}'
        if stream_timeout is not None:
            query_str += f'&stream_timeout={stream_timeout}'
        if stream_keep_alive_limit is not None:
            query_str += f'&stream_keep_alive_limit={stream_keep_alive_limit}'
        if query_str != '':
            page += '?' + query_str[1:]

        if 'Accept-Encoding' in headers:
            del (headers['Accept-Encoding'])

        response = self.session.get(url=page, headers=headers,
                                    stream=True)

        # response = requests.get(page, headers=headers, stream=True)
        if response.status_code not in [200]:
            response_content_str = response.content.decode('utf-8')
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription_events_stream. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        return NakadiStream(response)

    def get_event_type_partitions(self, event_type_name):
        """
        GET /event-types/{name}/partitions
        :param event_type_name:
        :return:
        """
        page = f"{self.nakadi_url}/event-types/{event_type_name}/partitions"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type_partitions. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def get_event_type_partition(self, event_type_name, partition_id):
        """
        GET /event-types/{name}/partitions/{partition}
        :param event_type_name:
        :param partition_id:
        :return:
        """
        page = f"{self.nakadi_url}/event-types/{event_type_name}/partitions/{partition_id}"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type_partition. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def get_subscriptions(self, owning_application=None, event_type=None,
                          limit=20,
                          offset=0):
        """
        GET /event-types/{name}/partitions/{partition}
        :param owning_application:
        :param event_type:
        :param limit:
        :param offset:
        :return:
        """
        self.assert_it(limit >= 1,
                       NakadiException(code=1, msg='limit must be >=1'))
        self.assert_it(limit <= 1000,
                       NakadiException(code=1, msg='limit must be <=1000'))
        self.assert_it(offset >= 0,
                       NakadiException(code=1, msg='offset must be >=0'))
        page = f"{self.nakadi_url}/subscriptions"
        query_str = "?limit=" + str(limit) + '&offset=' + str(offset)
        if owning_application is not None:
            query_str += f"&owning_application={owning_application}"
        if event_type is not None:
            query_str += reduce(
                lambda reduced, item: reduced + "&event_type=" + item,
                event_type, '')
        page += query_str
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscriptions. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def get_next_subscriptions(self, subscriptions_response):
        if 'next' not in subscriptions_response['_links']:
            return None
        page = f"{self.nakadi_url}{ subscriptions_response['_links']['next']['href']}"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscriptions. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def get_prev_subscriptions(self, subscriptions_response):
        if 'prev' not in subscriptions_response['_links']:
            return None
        page = f"{self.nakadi_url}{subscriptions_response['_links']['prev']['href']}"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscriptions. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def create_subscription(self, subscription_data_map):
        """
        POST /subscriptions
        :param subscription_data_map:
        :return:
        """
        page = f"{self.nakadi_url}/subscriptions"
        response = self.session.post(page,
                                     json=subscription_data_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200, 201]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during create_subscription. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def create_subscription_v2(self, subscription_data_map):
        """
        POST /subscriptions
        :param subscription_data_map:
        :return:
        """
        page = f"{self.nakadi_url}/subscriptions"
        response = self.session.post(page,
                                     json=subscription_data_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200, 201]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during create_subscription. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return (response.status_code, result_map)

    def get_subscription(self, subscription_id):
        """
        GET /subscriptions
        :param subscription_id:
        :return:
        """
        page = f"{self.nakadi_url}/subscriptions/{subscription_id}"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def delete_subscription(self, subscription_id):
        """
        DELETE /subscriptions/{subscription_id}
        :param subscription_id:
        :return:
        """
        page = f"{self.nakadi_url}/subscriptions/{subscription_id}"
        response = self.session.delete(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [204]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during delete_subscription. "
                    + f"Message from server:{response.status_code} {response_content_str}")

    def get_subscription_events_stream(self,
                                       subscription_id,
                                       max_uncommitted_events=None,
                                       batch_limit=None,
                                       stream_limit=None,
                                       batch_flush_timeout=None,
                                       stream_timeout=None,
                                       stream_keep_alive_limit=None,
                                       commit_timeout=None):
        """
        GET /subscriptions/{subscription_id}/events
        :param subscription_id:
        :param max_uncommitted_events:
        :param batch_limit:
        :param stream_limit:
        :param batch_flush_timeout:
        :param stream_timeout:
        :param stream_keep_alive_limit:
        :return: NakadiStream
        """
        page = f"{self.nakadi_url}/subscriptions/{subscription_id}/events"
        query_str = ''
        if max_uncommitted_events is not None:
            query_str += f'&max_uncommitted_events={max_uncommitted_events}'
        if batch_limit is not None:
            query_str += f'&batch_limit={batch_limit}'
        if stream_limit is not None:
            query_str += f'&stream_limit={stream_limit}'
        if batch_flush_timeout is not None:
            query_str += f'&batch_flush_timeout={batch_flush_timeout}'
        if stream_timeout is not None:
            query_str += f'&stream_timeout={stream_timeout}'
        if stream_keep_alive_limit is not None:
            query_str += f'&stream_keep_alive_limit={stream_keep_alive_limit}'
        if commit_timeout is not None:
            query_str += f'&commit_timeout={commit_timeout}'
        if query_str != '':
            page += '?' + query_str[1:]

        headers = copy.copy(self.session.headers)
        if 'Accept-Encoding' in headers:
            del (headers['Accept-Encoding'])
        response = self.session.get(url=page, headers=headers,
                                    stream=True)
        if response.status_code not in [200]:
            response_content_str = response.content.decode('utf-8')
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription_events_stream. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        return NakadiStream(response)

    def get_subscription_stats(self, subscription_id, show_time_lag=False):
        """
        GET /subscriptions/{subscription_id}/stats
        :param subscription_id:
        :param show_time_lag:
        :return:
        """
        page = f"{self.nakadi_url}/subscriptions/{subscription_id}/stats?show_time_lag={str(show_time_lag).lower()}"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription_stats. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def get_subscription_cursors(self, subscription_id):
        """
        GET /subscriptions/{subscription_id}/cursors
        :param subscription_id:
        :return:
        """
        page = f"{self.nakadi_url}/subscriptions/{subscription_id}/cursors"
        response = self.session.get(page)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription_stats. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        result_map = json.loads(response_content_str)
        return result_map

    def commit_subscription_cursors(self, subscription_id, stream_id, cursors):
        """
        POST /subscriptions/{subscription_id}/cursors
        :param subscription_id:
        :param stream_id:
        :param cursors:
        :return:
        """
        page = f"{self.nakadi_url}/subscriptions/{subscription_id}/cursors"
        headers = copy.copy(self.session.headers)
        headers["X-Nakadi-StreamId"] = stream_id
        cursors_data = {'items': cursors}
        response = self.session.post(page, headers=headers,
                                     json=cursors_data)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [204]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during commit_subscription_cursors. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        return True

    def reset_subscription_cursors(self, subscription_id, cursors):
        """
        PATCH /subscriptions/{subscription_id}/cursors
        :param subscription_id:
        :param cursors:
        :return:
        """
        page = f"{self.nakadi_url}/subscriptions/{subscription_id}/cursors"
        cursors_data = {'items': cursors}
        response = self.session.patch(page, json=cursors_data)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [204]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during reset_subscription_cursors. "
                    + f"Message from server:{response.status_code} {response_content_str}")
        return True
