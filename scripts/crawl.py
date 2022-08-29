#!/usr/bin/env python3

from __future__ import annotations
import asyncio

import json
import socket
import sys
from datetime import datetime as dt
from datetime import timezone as tz
from queue import Queue

from typing import Any, Union
from typing import Callable, Dict, List, Set, Tuple
from typing import NewType, overload
if sys.version_info >= (3, 8):
    from typing import Literal, TypedDict
else:
    from typing_extensions import Literal, TypedDict


class RequestFailed(Exception):
    pass


class TimeoutExceed(RequestFailed):
    pass


# gives the option to get data from an external server instead and send that
# if no options given it will default to localhost instead
SOCKADDR: Union[Tuple[str, int], str]
if len(sys.argv) == 3:
    SOCKTYPE = socket.AF_INET
    SOCKADDR = (sys.argv[1], int(sys.argv[2]))
elif len(sys.argv) == 2:
    SOCKTYPE = socket.AF_UNIX
    SOCKADDR = sys.argv[1]
else:
    SOCKTYPE = socket.AF_UNIX
    SOCKADDR = "/var/run/yggdrasil.sock"


NodeAddr = NewType("NodeAddr", str)
NodeKey = NewType("NodeKey", str)


class RawAPIResponse(TypedDict):
    status: Literal["success", "error"]
    request: Dict
    response: Dict


class SelfInfo(TypedDict):
    address: NodeAddr
    build_name: str
    build_version: str
    coords: List[int]
    key: NodeKey
    subnet: str


class RemoteSelfInfo(TypedDict):
    coords: str
    key: NodeKey


class KeysDict(TypedDict):
    keys: List[NodeKey]


class NodeSummary(TypedDict):
    address: NodeAddr
    coords: List[int]
    nodeinfo: Dict
    peers: List[NodeKey]
    dht: List[NodeKey]
    time: Union[dt, float]


@overload
async def doRequest(endpoint: Literal["getSelf"]) -> SelfInfo: ...
@overload  # noqa
async def doRequest(
    endpoint: Literal["getNodeInfo"], *, key: NodeKey
) -> Tuple[NodeAddr, Any]: ...
@overload  # noqa
async def doRequest(
    endpoint: Literal["debug_remoteGetSelf"], *, key: NodeKey
) -> RemoteSelfInfo: ...
@overload  # noqa
async def doRequest(
    endpoint: Literal["debug_remoteGetPeers"], *, key: NodeKey
) -> List[NodeKey]: ...
@overload  # noqa
async def doRequest(
    endpoint: Literal["debug_remoteGetDHT"], *, key: NodeKey
) -> List[NodeKey]: ...

async def doRequest(endpoint: str, **params):  # noqa
    response = None
    request_body = {
        "request": endpoint,
        **params,
        "keepalive": False
    }
    request_repr = json.dumps(request_body)
    request = request_repr.encode("utf-8")
    try:
        ygg = socket.socket(SOCKTYPE, socket.SOCK_STREAM)
        ygg.connect(SOCKADDR)
        reader, writer = await asyncio.open_connection(sock=ygg)
        writer.write(request)
        raw = await reader.read()
    except OSError as e:
        raise RequestFailed from e
    data: RawAPIResponse = json.loads(raw)
    status = data["status"]
    response = data["response"]
    if status == "error":
        params_repr = ", ".join(f"{k}={repr(v)}" for k, v in params.items())
        request_repr = f"{endpoint}({params_repr})"
        if response["error"] == "timeout":
            raise TimeoutError(f"{request_repr} timed out.")
        raise RequestFailed(f"{request_repr} request failed. {response['error']}.")
    postprocessor = RESPONSE_POSTPROCESS.get(endpoint)
    if postprocessor is not None:
        response = postprocessor(response)
    return response


# Bunch of API response postprocessing functions

def process_getself(response: Dict) -> SelfInfo:
    self_info = response["self"]
    addr, info = self_info.popitem()
    assert not self_info  # check that there was exactly one record in dict
    info["address"] = addr
    return info


def process_getnodeinfo(response: Dict) -> Tuple[NodeAddr, Any]:
    addr, nodeinfo = response.popitem()
    assert not response  # check that there was exactly one record in dict
    return addr, nodeinfo


def crutch(response: Dict) -> Any:
    _, value = response.popitem()
    assert not response  # check that there was exactly one record in dict
    return value


def cruth_for_keys(response: Dict[Any, KeysDict]) -> List[NodeKey]:
    _, keys_mapping = response.popitem()
    assert not response  # check that there was exactly one record in dict
    keys = keys_mapping.get("keys", [])
    return keys


RESPONSE_POSTPROCESS: Dict[str, Callable[[dict], Any]] = {
    "getSelf": process_getself,
    "getNodeInfo": process_getnodeinfo,
    "debug_remoteGetSelf": crutch,
    "debug_remoteGetPeers": cruth_for_keys,
    "debug_remoteGetDHT": cruth_for_keys
}


async def visit(key: NodeKey) -> Tuple[NodeSummary, Set[NodeKey]]:
    requests = [
        doRequest("getNodeInfo", key=key),
        doRequest("debug_remoteGetSelf", key=key),
        doRequest("debug_remoteGetPeers", key=key),
        doRequest("debug_remoteGetDHT", key=key)
    ]
    try:
        (addr, nodeinfo), details, peers, dht = await asyncio.gather(*requests)
    except RequestFailed as e:
        time_raw = dt.now(tz=tz.utc)
        time = time_raw.astimezone()
        return {"time": time, "error": type(e).__name__}, []  # type: ignore
    time_raw = dt.now(tz=tz.utc)
    time = time_raw.astimezone()
    coords_repr = details["coords"]
    coords_repr = coords_repr.strip("[]")  # cuts off brackets
    coords = [int(port) for port in coords_repr.split() if port]
    summary = NodeSummary(
        address=addr, coords=coords,
        nodeinfo=nodeinfo, peers=peers, dht=dht,
        time=time
    )
    node_seen = {*peers, *dht}
    return summary, node_seen


async def crawl() -> Dict[NodeKey, NodeSummary]:
    known: Set[NodeKey] = set()
    visited: Dict[NodeKey, NodeSummary] = {}
    queue: Queue[NodeKey] = Queue()
    self_info = await doRequest("getSelf")
    queue.put(self_info['key'])
    while not queue.empty():
        planned_keys = []
        while not queue.empty():
            pubkey = queue.get()
            known.add(pubkey)
            if pubkey in visited:
                continue
            planned_keys.append(pubkey)
        planned = (visit(pubkey) for pubkey in planned_keys)
        results = await asyncio.gather(*planned)
        for node_summary, newly_known in results:
            visited[pubkey] = node_summary
            newly_known -= known
            for key in newly_known:
                queue.put(key)
                known.add(key)
    return visited


class MapEncoder(json.JSONEncoder):

    def default(self, o: Any) -> Any:
        if isinstance(o, dt):
            return o.isoformat()
        return super().default(o)


if __name__ == "__main__":
    if sys.platform == "win32":
        from asyncio import WindowsProactorEventLoopPolicy
        windows_policy = WindowsProactorEventLoopPolicy()
        asyncio.set_event_loop_policy(windows_policy)
    network_map = asyncio.run(crawl())
    json.dump(network_map, sys.stdout, indent=2, cls=MapEncoder)
    sys.stdout.flush()
