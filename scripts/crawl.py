#!/usr/bin/env python3

from __future__ import annotations

import json
import socket
import sys
from datetime import datetime as dt
from datetime import timezone as tz
from itertools import chain
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
def doRequest(
    endpoint: Literal["getSelf"], keepalive: bool = True
) -> SelfInfo: ...
@overload  # noqa
def doRequest(
    endpoint: Literal["getNodeInfo"], keepalive: bool = True,
    *, key: NodeKey
) -> Tuple[NodeAddr, Any]: ...
@overload  # noqa
def doRequest(
    endpoint: Literal["debug_remoteGetSelf"], keepalive: bool = True,
    *, key: NodeKey
) -> RemoteSelfInfo: ...
@overload  # noqa
def doRequest(
    endpoint: Literal["debug_remoteGetPeers"], keepalive: bool = True,
    *, key: NodeKey
) -> List[NodeKey]: ...
@overload  # noqa
def doRequest(
    endpoint: Literal["debug_remoteGetDHT"], keepalive: bool = True,
    *, key: NodeKey
) -> List[NodeKey]: ...

def doRequest(endpoint: str, keepalive: bool = True, **params):  # noqa
    response = None
    request_body = {
        "request": endpoint,
        "keepalive": keepalive,
        **params
    }
    request_repr = json.dumps(request_body)
    request = request_repr.encode("utf-8")
    try:
        ygg = socket.socket(SOCKTYPE, socket.SOCK_STREAM)
        ygg.connect(SOCKADDR)
        ygg.send(request)
        raw = ygg.recv(1024**2)
    except OSError as e:
        raise RequestFailed from e
    data: RawAPIResponse = json.loads(raw)
    status = data["status"]
    response = data["response"]
    if status == "error":
        if keepalive:
            params["keepalive"] = keepalive
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
    addr, info = response.popitem()
    info["address"] = addr
    return info


def process_getnodeinfo(response: Dict) -> Tuple[NodeAddr, Any]:
    addr, nodeinfo = response.popitem()
    return addr, nodeinfo


def crutch(response: Dict) -> Any:
    _, value = response.popitem()
    return value


def cruth_for_keys(response: Dict[Any, KeysDict]) -> List[NodeKey]:
    _, keys_mapping = response.popitem()
    return keys_mapping.get("keys", [])


RESPONSE_POSTPROCESS: Dict[str, Callable[[dict], Any]] = {
    "getSelf": process_getself,
    "getNodeInfo": process_getnodeinfo,
    "debug_remoteGetSelf": crutch,
    "debug_remoteGetPeers": cruth_for_keys,
    "debug_remoteGetDHT": cruth_for_keys
}


def crawl() -> Dict[NodeKey, NodeSummary]:
    known: Set[NodeKey] = set()
    visited: Dict[NodeKey, NodeSummary] = {}
    queue: Queue[NodeKey] = Queue()
    self_info = doRequest("getSelf")
    queue.put(self_info['key'])
    while not queue.empty():
        pubkey = queue.get()
        known.add(pubkey)
        if pubkey in visited:
            continue
        try:
            addr, nodeinfo = doRequest("getNodeInfo", key=pubkey)
            details = doRequest("debug_remoteGetSelf", key=pubkey)
            peers = doRequest("debug_remoteGetPeers", key=pubkey)
            dht = doRequest("debug_remoteGetDHT", key=pubkey)
            time_raw = dt.now(tz=tz.utc)
            time = time_raw.astimezone()
        except RequestFailed as e:
            visited[pubkey] = {"time": time, "error": type(e).__name__}  # type: ignore  # noqa
            continue
        coords = details["coords"]
        visited[pubkey] = NodeSummary(
            address=addr, coords=coords,
            nodeinfo=nodeinfo, peers=peers, dht=dht,
            time=time
        )
        for key in chain(peers, dht):
            if key not in known:
                queue.put(key)
            known.add(key)
    return visited


class MapEncoder(json.JSONEncoder):

    def default(self, o: Any) -> Any:
        if isinstance(o, dt):
            return o.isoformat()
        return super().default(o)


network_map = crawl()
json.dump(network_map, sys.stdout, indent=2, cls=MapEncoder)
sys.stdout.flush()
