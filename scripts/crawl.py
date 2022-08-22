#!/usr/bin/env python3

from __future__ import annotations

import json
import socket
import sys
import time

from typing import Dict, List, NewType, Optional, Set, Tuple, Union, overload
if sys.version_info >= (3, 8):
    from typing import Literal, TypedDict
else:
    from typing_extensions import Literal, TypedDict

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


def getNodeInfoRequest(key):
    return '{{"keepalive":true, "request":"getNodeInfo", "key":"{}"}}'.format(key)


NodeAddr = NewType("NodeAddr", str)
NodeKey = NewType("NodeKey", str)


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


def getSelfRequest(key):
    return '{{"keepalive":true, "request":"debug_remoteGetSelf", "key":"{}"}}'.format(key)


def getPeersRequest(key):
    return '{{"keepalive":true, "request":"debug_remoteGetPeers", "key":"{}"}}'.format(key)


def getDHTRequest(key):
    return '{{"keepalive":true, "request":"debug_remoteGetDHT", "key":"{}"}}'.format(key)


def doRequest(req):
    try:
        ygg = socket.socket(SOCKTYPE, socket.SOCK_STREAM)
        ygg.connect(SOCKADDR)
        ygg.send(req)
        data = json.loads(ygg.recv(1024*15))
        return data
    except:
        return None


visited: Set[NodeKey] = set()  # Add nodes after a successful lookup response
rumored: Set[NodeKey] = set()  # Add rumors about nodes to ping
timedout: Set = set()


def handleNodeInfoResponse(publicKey, data):
    global visited
    global rumored
    global timedout
    if publicKey in visited:
        return
    if not data:
        return
    if 'response' not in data:
        return
    out = dict()
    for addr, v in data['response'].iteritems():
        out['address'] = addr
        out['nodeinfo'] = v
    selfInfo = doRequest(getSelfRequest(publicKey))
    if 'response' in selfInfo:
        for _, v in selfInfo['response'].iteritems():
            if 'coords' in v:
                out['coords'] = v['coords']
    peerInfo = doRequest(getPeersRequest(publicKey))
    if 'response' in peerInfo:
        for _, v in peerInfo['response'].iteritems():
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
    dhtInfo = doRequest(getDHTRequest(publicKey))
    if 'response' in dhtInfo:
        for _, v in dhtInfo['response'].iteritems():
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
# End handleResponse


# Get self info
selfInfo = doRequest('{"keepalive":true, "request":"getSelf"}')
for k, v in selfInfo['response']['self'].iteritems():
    rumored.add(v['key'])

# Initialize dicts of visited/rumored nodes
#for k,v in selfInfo['response']['self'].iteritems(): rumored[k] = v

# Loop over rumored nodes and ping them, adding to visited if they respond
print('{"yggnodes": {')
while len(rumored) > 0:
    for k in rumored:
        handleNodeInfoResponse(k, doRequest(getNodeInfoRequest(k)))
        break
    rumored.remove(k)
print('\n}}')
# End

# TODO do something with the results

# print visited
# print timedout
