
#For developers

## Pull requests

**IMPORTANT** 
Tests are recently added to the project. If your PR is rooting from an older commit,
please rebase your changes. PRs that are rooting from older commits will not be reviewed
 by project maintainers.
 
**IMPORTANT** 
Project maintainers may ask contributors to add more tests regarding their changes. Your changes may not be 
merged until requirements are satisfied. 


## Development environment
You should have the following for development:
* make utility
* conda (miniconda is sufficient)
* python=3.6 with conda
* docker and docker-compose (for tests)
  
## Integration testing against Nakadi
`tests/conftest.py` has many constants that you may need to change according to what and how
you test.

```
URL_INTEGRATION_TEST_SERVER_PORT = '8080'
URL_INTEGRATION_TEST_SERVER_HOST = 'localhost'
# If you are testing against a remote nakadi server set False
INTEGRATION_TEST_SERVER_LOCAL_DOCKER = True
URL_INTEGRATION_TEST_SERVER = f'http://{URL_INTEGRATION_TEST_SERVER_HOST}:{URL_INTEGRATION_TEST_SERVER_PORT}'
NAKADI_GIT_REPO = 'https://github.com/zalando/nakadi.git'
# If you test against a different nakadi version please change
NAKADI_TEST_TAG = 'r3.3.10-2019-12-05'
```


