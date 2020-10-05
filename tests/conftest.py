import time

import json
import pytest
import subprocess
from subprocess import Popen
from tempfile import TemporaryDirectory, NamedTemporaryFile
from datetime import datetime

from pyNakadi import NakadiClient, NakadiException
from itertools import count

URL_INTEGRATION_TEST_SERVER_PORT = '8080'
URL_INTEGRATION_TEST_SERVER_HOST = 'localhost'
# If you are testing against a remote nakadi server set False
INTEGRATION_TEST_SERVER_LOCAL_DOCKER = True
URL_INTEGRATION_TEST_SERVER = f'http://{URL_INTEGRATION_TEST_SERVER_HOST}:{URL_INTEGRATION_TEST_SERVER_PORT}'
NAKADI_GIT_REPO = 'https://github.com/zalando/nakadi.git'
# If you test against a different nakadi version please change
NAKADI_TEST_TAG = 'r3.3.10-2019-12-05'


@pytest.fixture(scope="module")
def nakadi_url():
    if INTEGRATION_TEST_SERVER_LOCAL_DOCKER:
        pass
        # build docker
        temp_dir = TemporaryDirectory()
        docker_log_file = NamedTemporaryFile()
        print(f'Logging into {docker_log_file.name}')
        print("Cloning nakadi repo ...")
        git_clone_repo(NAKADI_GIT_REPO, temp_dir.name)
        print(f'Checking out {NAKADI_TEST_TAG} ...')
        git_checkout(temp_dir.name, NAKADI_TEST_TAG)
        print('Building nakadi ...')
        build_nakadi(temp_dir.name)
        print('Starting nakadi stack ...')
        start_nakadi_stack(temp_dir.name, docker_log_file)
        print('Waiting for nakadi to respond ...')
        while not do_metrics_exist(URL_INTEGRATION_TEST_SERVER):
            time.sleep(5)
    print('Initializing nakadi ...')
    initialize_nakadi(URL_INTEGRATION_TEST_SERVER)
    print('Initialized nakadi')
    yield URL_INTEGRATION_TEST_SERVER
    if INTEGRATION_TEST_SERVER_LOCAL_DOCKER:
        print()
        print('Stopping nakadi stack ...')
        p = stop_nakadi_stack(temp_dir.name)
        temp_dir.cleanup()


def git_clone_repo(repo_addr, parent_dir):
    subprocess.run(f'git clone {repo_addr} .', shell=True, cwd=parent_dir)


def git_checkout(repo_dir, ref):
    subprocess.run(f'git checkout {ref} -b current_build_br', shell=True, cwd=repo_dir)


def build_nakadi(repo_dir):
    subprocess.run(f'./gradlew build -x test', shell=True, cwd=repo_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def start_nakadi_stack(repo_dir, log_file):
    Popen(args=f'docker-compose -f docker-compose.yml up', shell=True, cwd=repo_dir, stdout=log_file,
          stderr=log_file)


def stop_nakadi_stack(repo_dir):
    p = Popen(args=f'docker-compose -f docker-compose.yml down', shell=True, cwd=repo_dir, stdout=subprocess.PIPE,
              stderr=subprocess.PIPE)
    p.wait()


def do_metrics_exist(server):
    try:
        c = NakadiClient('dummy_token', server)
        metrics = c.get_metrics()
        return 'counters' in metrics
    except:
        return False


def initialize_nakadi(server):
    c = NakadiClient('dummy_token', server)
    try:
        # create event types
        for i in range(0, 4):
            event_type_name = f'pyNakadi_test_event_type_{i}'
            c.create_event_type(testing_event_type_common(event_type_name))

        # create events
        eids = [
            'e6841941-caec-449d-afb3-43b071b595b4',
            '0b66f3de-9eba-4672-8c80-54325d78d828',
            '652d6ac3-6f31-4520-883d-b3658ccf37b3',
            'efd6f303-ea11-499b-ae35-e37c96c1cf8e',
            '657c0b6f-06bf-454d-8104-e6531942742b',
            '7a0cbc35-ff8d-478e-90ff-070704a915e4',
            '31955e82-ecc5-449d-a14e-cc58276866c1',
            'ff475e80-d35b-4641-8740-408a0557622c',
            '6bc083d9-e45c-4f3b-80fa-e140a5b8b6f8',
            'c6e16ea0-5f66-4f5c-a98a-c0823004bbbb',

        ]

        events = [testing_event(eid, str(i)) for i, eid in zip(count(), eids)]

        c.post_events(
            'pyNakadi_test_event_type_0',
            events
        )


    except NakadiException as e:
        print(e)
        raise e


def escaped_json_str(dicty):
    return json.dumps(dicty).replace('"', '\"')


def testing_schema():
    return {
        "type": 'json_schema',
        'schema': escaped_json_str({
            "properties": {
                "field_1": {
                    "type": "string"
                },
                "field_2": {
                    "type": "string"
                }
            }
        })
    }


def testing_event_statistic():
    return {
        "messages_per_minute": 1000,
        "message_size": 5,
        "read_parallelism": 2,
        "write_parallelism": 2
    }


def testing_event_type_common(event_type_name):
    return {
        'name': event_type_name,
        'owning_application': 'app1',
        'category': 'business',
        "enrichment_strategies": ["metadata_enrichment"],
        "partition_key_fields": ["field_1"],
        "partition_strategy": "hash",
        "default_statistic": testing_event_statistic(),
        'schema': testing_schema(),
    }


def testing_event(eid, field_1):
    event_time = '2020-02-01T20:00:00.000000+00:00'
    return {
        "metadata": {
            "eid": eid,
            "occurred_at": event_time
        },
        "field_1": field_1,
        "field_2": "value2"
    }


def u8(msg):
    return msg.decode('utf-8')
