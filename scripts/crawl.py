#!/usr/bin/env python3

from __future__ import annotations

import json
import socket
import sys
import time

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


class SelfResponse(TypedDict):
    self: Dict[NodeAddr, NodeInfo]


class NodeInfo(TypedDict):
    build_name: str
    build_version: str
    coords: List[int]
    key: NodeKey
    subnet: str


class RemoteSelfInfo(TypedDict):
    coords: str
    key: NodeKey


class RemotePeers(TypedDict):
    keys: List[NodeKey]


class RemoteDHT(TypedDict):
    keys: List[NodeKey]


class NodeSummary(TypedDict):
    address: NodeAddr
    coords: List[int]
    nodeinfo: NodeInfo
    peers: List[NodeKey]
    dht: List[NodeKey]
    time: float


@overload
def doRequest(
    endpoint: Literal["getSelf"], keepalive: bool = True
) -> SelfResponse: ...
@overload  # noqa
def doRequest(
    endpoint: Literal["getNodeInfo"], keepalive: bool = True,
    *, key: NodeKey
) -> Dict[NodeAddr, NodeInfo]: ...
@overload  # noqa
def doRequest(
    endpoint: Literal["debug_remoteGetSelf"], keepalive: bool = True,
    *, key: NodeKey
) -> Dict[NodeAddr, RemoteSelfInfo]: ...
@overload  # noqa
def doRequest(
    endpoint: Literal["debug_remoteGetPeers"], keepalive: bool = True,
    *, key: NodeKey
) -> Dict[NodeAddr, RemotePeers]: ...
@overload  # noqa
def doRequest(
    endpoint: Literal["debug_remoteGetDHT"], keepalive: bool = True,
    *, key: NodeKey
) -> Dict[NodeAddr, RemoteDHT]: ...

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
    return data.get("response")


visited: Set[NodeKey] = set()  # Add nodes after a successful lookup response
rumored: Set[NodeKey] = set()  # Add rumors about nodes to ping
timedout: Set = set()


def handleNodeInfoResponse(publicKey: NodeKey, info: Dict[NodeAddr, NodeInfo]):
    global visited
    global rumored
    global timedout
    if publicKey in visited:
        return
    if not info:
        return
    out: NodeSummary = dict()
    for addr, details in info.items():
        out['address'] = addr
        out['nodeinfo'] = details
    selfInfo = doRequest("debug_remoteGetSelf", key=publicKey)
    if selfInfo is not None:
        for node_info in selfInfo.values():
            if 'coords' in node_info:
                out['coords'] = node_info['coords']
    peerInfo = doRequest("debug_remoteGetPeers", key=publicKey)
    if peerInfo is not None:
        for v in peerInfo.values():
            if 'keys' not in v:
                continue
            peers = v['keys']
            for key in peers:
                if key in visited:
                    continue
                if key in timedout:
                    continue
                rumored.add(key)
            out['peers'] = peers
    dhtInfo = doRequest("debug_remoteGetDHT", key=publicKey)
    if dhtInfo is not None:
        for v in dhtInfo.values():
            if 'keys' in v:
                dht = v['keys']
                for key in dht:
                    if key in visited:
                        continue
                    if key in timedout:
                        continue
                    rumored.add(key)
                out['dht'] = dht
    out['time'] = time.time()
    if len(visited) > 0:
        sys.stdout.write(",\n")
    sys.stdout.write('"{}": {}'.format(publicKey, json.dumps(out)))
    sys.stdout.flush()
    visited.add(publicKey)


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


# Get self info
selfInfo = doRequest("getSelf")
if selfInfo:
    for self_info in selfInfo['self'].values():
        rumored.add(self_info['key'])

# Initialize dicts of visited/rumored nodes
# for k,v in selfInfo['self'].iteritems(): rumored[k] = v

# Loop over rumored nodes and ping them, adding to visited if they respond
print('{"yggnodes": {')
while len(rumored) > 0:
    for k in rumored:
        nodeinfo = doRequest("getNodeInfo", key=k)
        if not nodeinfo:
            continue
        handleNodeInfoResponse(k, nodeinfo)
        break
    rumored.remove(k)
print('\n}}')
# End

# TODO do something with the results

# print visited
# print timedout
