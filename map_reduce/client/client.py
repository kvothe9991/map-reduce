from typing import Any, TypeVar
from map_reduce.client.server_interface import ServerInterface as server


data = []

def map(in_key: str, in_value: Any) -> tuple[str, Any]:
    pass

def reduce(out_key: str, inter_value: Any) -> list[Any]:
    pass


if __name__ == "__main__":
    if daemon := server.startup(data, map, reduce):
        server.await_results()
        print( server.results )