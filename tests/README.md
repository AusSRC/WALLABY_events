# Tests

We have included test code to make sure the system behaves as expected in response to events.

These tests are run outside of the production environment, ideally in a testing server. To get the tests running first you will need to deploy the environment, which includes a RabbitMQ and PostgreSQL (with WALLABY schema) servers. This can be done with the `docker-compose.test.yml` script provided:

```
docker-compose up -f etc/docker-compose.test.yml -d
```

It is also recommended to run the tests in a virtual environment. You will need to install `pytest` and `pytest-asyncio`. 

```
source venv/bin/activate
pip install -r tests/requirements.txt
```

Then you can run all of the tests with

```
export PYTHONPATH=.
pytest
```

## Functional tests



## Unittests

We have some unittests for the `logic.py` module.