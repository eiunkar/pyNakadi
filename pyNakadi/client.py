import json
import socket
import uuid
from functools import reduce

import requests


class NakadiException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg


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

    def set_token(self, token):
        """
        Sets token for this client
        :param token: token string to be used
        :return:
        """
        self.token = token

    def authorization_header(self, headers=None):
        """
        Returns authorization header of this client
        :param headers:
        :return: authorization header dict
        """
        if headers is None:
            headers = dict()
        headers['Authorization'] = 'Bearer ' + self.token
        return headers

    @classmethod
    def assert_it(cls, condition, exception):
        if not condition:
            raise exception

    def json_content_header(self, headers=None):
        """
        Returns authorization header of this clients
        :param headers:
        :return: authorization header dict
        """
        if headers is None:
            headers = dict()
        headers["Content-type"] = "application/json"
        return headers

    def get_event_types(self):
        """
        GET /event-types
        :return:
        """
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        page = "{}/event-types".format(self.nakadi_url)
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_types. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def create_event_type(self, event_type_data_map):
        """
        POST /event-types
        :param event_type_data_map:
        :return:
        """
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        page = "{}/event-types".format(self.nakadi_url)
        response = requests.post(page, headers=headers,
                                 json=event_type_data_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [201]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during create_event_type. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        return True

    def get_event_type(self, event_type_name):
        """
        GET /event-types/{name}
        :param event_type_name:
        :return:
        """
        headers = self.authorization_header()
        page = "{}/event-types/{}".format(self.nakadi_url, event_type_name)
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def update_event_type(self, event_type_name, event_type_data_map):
        """
        PUT /event-types/{name}
        :param event_type_name:
        :param event_type_data_map:
        :return:
        """
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        page = "{}/event-types/{}".format(self.nakadi_url, event_type_name)
        response = requests.put(page, headers=headers, json=event_type_data_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during update_event_type. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def delete_event_type(self, event_type_name):
        """
        DELETE /event-types/{name}
        :param event_type_name:
        :return:
        """
        headers = self.authorization_header()
        page = "{}/event-types/{}".format(self.nakadi_url, event_type_name)
        response = requests.delete(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during delete_event_type. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        return True

    def get_event_type_cursor_distances(self, event_type_name, query_map):
        """
        POST /event-types/{name}/cursor-distances
        :param event_type_name:
        :param query_map:
        :return:
        """
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        page = "{}/event-types/{}/cursor-distances".format(self.nakadi_url,
                                                           event_type_name)
        response = requests.post(page, headers=headers, json=query_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type_cursor_distances. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def get_event_type_cursor_lag(self, event_type_name, cursors_map):
        """
        POST /event-types/{name}/cursors-lag
        :param event_type_name:
        :param cursors_map:
        :return:
        """
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        page = "{}/event-types/{}/cursor-lag".format(self.nakadi_url,
                                                     event_type_name)
        response = requests.post(page, headers=headers, json=cursors_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type_cursor_lag. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def post_events(self, event_type_name, events):
        """
        POST /event-types/{name}/events
        :param event_type_name:
        :param events:
        :return:
        """
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        page = "{}/event-types/{}/events".format(self.nakadi_url,
                                                 event_type_name)
        response = requests.post(page, headers=headers, json=events)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during post_events. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
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
        headers = self.authorization_header()
        if cursors is not None:
            headers['X-nakadi-cursors'] = json.dumps(cursors)
        page = "{}/event-types/{}/events".format(self.nakadi_url,
                                                 event_name)
        query_str = ''
        if batch_limit is not None:
            query_str += '&batch_limit={}'.format(batch_limit)
        if stream_limit is not None:
            query_str += '&stream_limit={}'.format(stream_limit)
        if batch_flush_timeout is not None:
            query_str += '&batch_flush_timeout={}'.format(batch_flush_timeout)
        if stream_timeout is not None:
            query_str += '&stream_timeout={}'.format(stream_timeout)
        if stream_keep_alive_limit is not None:
            query_str += '&stream_keep_alive_limit={}'.format(
                stream_keep_alive_limit)
        if query_str != '':
            page += '?' + query_str[1:]

        s = requests.Session()
        del (s.headers['Accept-Encoding'])
        response = s.request(method='GET', url=page, headers=headers,
                             stream=True)
        # response = requests.get(page, headers=headers, stream=True)
        if response.status_code not in [200]:
            response_content_str = response.content.decode('utf-8')
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription_events_stream. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        return NakadiStream(response)

    def get_event_type_partitions(self, event_type_name):
        """
        GET /event-types/{name}/partitions
        :param event_type_name:
        :return:
        """
        headers = self.authorization_header()
        page = "{}/event-types/{}/partitions".format(self.nakadi_url,
                                                     event_type_name)
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type_partitions. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def get_event_type_partition(self, event_type_name, partition_id):
        """
        GET /event-types/{name}/partitions/{partition}
        :param event_type_name:
        :param partition_id:
        :return:
        """
        headers = self.authorization_header()
        page = "{}/event-types/{}/partitions/{}".format(self.nakadi_url,
                                                        event_type_name,
                                                        partition_id)
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_event_type_partition. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
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
        headers = self.authorization_header()
        page = "{}/subscriptions".format(self.nakadi_url)
        query_str = "?limit=" + str(limit) + '&offset=' + str(offset)
        if owning_application is not None:
            query_str += "&owning_application=" + owning_application
        if event_type is not None:
            query_str += reduce(
                lambda reduced, item: reduced + "&event_type=" + item,
                event_type, '')
        page += query_str
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscriptions. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def get_next_subscriptions(self, subscriptions_response):
        if 'next' not in subscriptions_response['_links']:
            return subscriptions_response
        headers = self.authorization_header()
        page = '{}{}'.format(self.nakadi_url,
                             subscriptions_response['_links']['next']['href'])
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscriptions. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def get_prev_subscriptions(self, subscriptions_response):
        if 'prev' not in subscriptions_response['_links']:
            return subscriptions_response
        headers = self.authorization_header()
        page = '{}{}'.format(self.nakadi_url,
                             subscriptions_response['_links']['prev']['href'])
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscriptions. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def create_subscription(self, subscription_data_map):
        """
        POST /subscriptions
        :param subscription_data_map:
        :return:
        """
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        page = "{}/subscriptions".format(self.nakadi_url)
        response = requests.post(page, headers=headers,
                                 json=subscription_data_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200, 201]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during create_subscription. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def create_subscription_v2(self, subscription_data_map):
        """
        POST /subscriptions
        :param subscription_data_map:
        :return:
        """
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        page = "{}/subscriptions".format(self.nakadi_url)
        response = requests.post(page, headers=headers,
                                 json=subscription_data_map)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200, 201]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during create_subscription. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return (response.status_code, result_map)

    def get_subscription(self, subscription_id):
        """
        GET /subscriptions
        :param subscription_id:
        :return:
        """
        headers = self.authorization_header()
        page = "{}/subscriptions/{}".format(self.nakadi_url,
                                            subscription_id)
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def delete_subscription(self, subscription_id):
        """
        DELETE /subscriptions/{subscription_id}
        :param subscription_id:
        :return:
        """
        headers = self.authorization_header()
        page = "{}/subscriptions/{}".format(self.nakadi_url,
                                            subscription_id)
        response = requests.delete(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [204]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during delete_subscription. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))

    def get_subscription_events_stream(self,
                                       subscription_id,
                                       max_uncommitted_events=None,
                                       batch_limit=None,
                                       stream_limit=None,
                                       batch_flush_timeout=None,
                                       stream_timeout=None,
                                       stream_keep_alive_limit=None):
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
        headers = self.authorization_header()
        page = "{}/subscriptions/{}/events".format(self.nakadi_url,
                                                   subscription_id)
        query_str = ''
        if max_uncommitted_events is not None:
            query_str += '&max_uncommitted_events={}'.format(
                max_uncommitted_events)
        if batch_limit is not None:
            query_str += '&batch_limit={}'.format(batch_limit)
        if stream_limit is not None:
            query_str += '&stream_limit={}'.format(stream_limit)
        if batch_flush_timeout is not None:
            query_str += '&batch_flush_timeout={}'.format(batch_flush_timeout)
        if stream_timeout is not None:
            query_str += '&stream_timeout={}'.format(stream_timeout)
        if stream_keep_alive_limit is not None:
            query_str += '&stream_keep_alive_limit={}'.format(
                stream_keep_alive_limit)
        if query_str != '':
            page += '?' + query_str[1:]
        print(page)
        s = requests.Session()
        del (s.headers['Accept-Encoding'])
        response = s.request(method='GET', url=page, headers=headers,
                             stream=True)
        if response.status_code not in [200]:
            response_content_str = response.content.decode('utf-8')
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription_events_stream. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        return NakadiStream(response)

    def get_subscription_stats(self, subscription_id):
        """
        GET /subscriptions/{subscription_id}/stats
        :param subscription_id:
        :return:
        """
        headers = self.authorization_header()
        page = "{}/subscriptions/{}/stats".format(self.nakadi_url,
                                                  subscription_id)
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription_stats. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        result_map = json.loads(response_content_str)
        return result_map

    def get_subscription_cursors(self, subscription_id):
        """
        GET /subscriptions/{subscription_id}/cursors
        :param subscription_id:
        :return:
        """
        headers = self.authorization_header()
        page = "{}/subscriptions/{}/cursors".format(self.nakadi_url,
                                                    subscription_id)
        response = requests.get(page, headers=headers)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [200]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during get_subscription_stats. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
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
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        headers['X-Nakadi-StreamId'] = stream_id
        page = "{}/subscriptions/{}/cursors".format(self.nakadi_url,
                                                    subscription_id)
        cursors_data = {'items': cursors}
        response = requests.post(page, headers=headers,
                                 json=cursors_data)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [204]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during commit_subscription_cursors. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        return True

    def reset_subscription_cursors(self, subscription_id, cursors):
        """
        PATCH /subscriptions/{subscription_id}/cursors
        :param subscription_id:
        :param cursors:
        :return:
        """
        headers = self.authorization_header()
        headers = self.json_content_header(headers)
        page = "{}/subscriptions/{}/cursors".format(self.nakadi_url,
                                                    subscription_id)
        cursors_data = {'items': cursors}
        response = requests.patch(page, headers=headers,
                                  json=cursors_data)
        response_content_str = response.content.decode('utf-8')
        if response.status_code not in [204]:
            raise NakadiException(
                code=response.status_code,
                msg="Error during reset_subscription_cursors. "
                    + "Message from server:{} {}".format(response.status_code,
                                                         response_content_str))
        return True
