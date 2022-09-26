# A Python implementation of the map-reduce protocol.

## Usage:
### Requirements (once per system):

- Docker image: python:3.9.7-alpine
either (with internet connection) [~50mb]:
```
docker pull python:3.9.7-alpine
```
or (from provided tar image):
```
docker load -i <img_path>`
```

- Docker network named "distributed"
```bash
docker network create --driver=bridge --subnet=172.18.0.0/16 --ip-range=172.18.1.0/25  \
                      --gateway=172.18.1.254 distributed
```

## Usage (every time):

- Raise and tail `k` server containers (with cd on project root):
```bash
./scripts/batch_run server k -dt
```

- Startup client request.
```bash
./scripts/run_client
```
Which runs the dedicated python client script in `./map_reduce/client/client.py`, it requires the initial data to map of signature `[(IN_KEY, IN_VALUE)]`, as well as the `map` and `reduce` functions with the following signature:
```python
def map(in_key: IN_KEY, in_value: IN_VALUE) -> [(OUT_KEY, MID_VALUE)]:
    pass

def reduce(out_key: OUT_KEY, values: [MID_VALUE]) -> OUT_VAL:
    pass
```
The provided API then eventually returns through `ServerInterface.await_results()` the MapReduce procedure's result: a `dict` or key/value pair iterable of type signature `{ OUT_KEY: OUT_VAL }`.
