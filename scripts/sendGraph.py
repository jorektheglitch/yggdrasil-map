#!/usr/bin/env python3
# Based on Kyrias' sendGraph script. Requires Python 3, requests and cjdns.
# You can install them using pip: pip3 install cjdns requests
###############################################################################
# CONFIG

# URL where data is sent
#    www.fc00.org              for clearnet access
#    h.fc00.org                for hyperboria
#    [fc53:dcc5:e89d:9082:4097:6622:5e82:c654] for DNS-less access
import threading
import queue
from cjdns import admin_tools
from cjdns import key_utils
import cjdns
import requests
import argparse
import json
import traceback
import sys


URL = 'http://www.fc00.org/sendGraph'

# update your email address, so I can contact you in case something goes wrong
YOUR_EMAIL = 'your@email.here'


# ----------------------
# RPC connection details
# ----------------------

# If this is set to True connection details will be loaded from ~/.cjdnsadmin
cjdns_use_default = True

# otherwise these are used.
cjdns_ip = '127.0.0.1'
cjdns_port = 11234
cjdns_password = 'NONE'

###############################################################################


def main():
    parser = argparse.ArgumentParser(
        description='Submit nodes and links to fc00'
    )
    parser.add_argument(
        '-v', '--verbose',
        help='increase output verbosity',
        dest='verbose', action='store_true'
    )
    parser.set_defaults(verbose=False)
    args = parser.parse_args()

    con = connect()

    nodes = dump_node_store(con)
    edges = {}

    get_peer_queue = queue.Queue(0)
    result_queue = queue.Queue(0)

    for k in nodes:
        get_peer_queue.put(k)

    for i in range(8):
        t = threading.Thread(
            target=worker,
            args=[nodes, get_peer_queue, result_queue, args.verbose]
        )
        t.daemon = True
        t.start()

    for i in range(len(nodes)):
        peers, node_ip = result_queue.get()
        get_edges_for_peers(edges, peers, node_ip)

    send_graph(nodes, edges)
    sys.exit(0)


def worker(nodes, get_peer_queue, result, verbose=False):
    con = connect()

    while True:
        try:
            k = get_peer_queue.get_nowait()
        except queue.Empty:
            return

        node = nodes[k]
        if verbose:
            print('fetch', node)
        node_ip = node['ip']

        peers = get_all_peers(con, node['path'])

        result.put((peers, node_ip))


def connect():
    try:
        if cjdns_use_default:
            print('Connecting using default or ~/.cjdnsadmin credentials...')
            con = cjdns.connectWithAdminInfo()
        else:
            print('Connecting to port {:d}...'.format(cjdns_port))
            con = cjdns.connect(cjdns_ip, cjdns_port, cjdns_password)

        return con

    except:
        print('Connection failed!')
        print(traceback.format_exc())
        sys.exit(1)


def dump_node_store(con):
    nodes = dict()

    i = 0
    while True:
        res = con.NodeStore_dumpTable(i)

        if not 'routingTable' in res:
            break

        for n in res['routingTable']:
            if not all(key in n for key in ('addr', 'path', 'ip')):
                continue

            ip = n['ip']
            path = n['path']
            addr = n['addr']
            version = None
            if 'version' in n:
                version = n['version']

            nodes[ip] = {'ip': ip, 'path': path,
                         'addr': addr, 'version': version}

        if not 'more' in res or res['more'] != 1:
            break

        i += 1

    return nodes


def get_peers(con, path, nearbyPath=''):
    formatted_path = path
    if nearbyPath:
        formatted_path = '{:s} (nearby {:s})'.format(path, nearbyPath)

    i = 1
    retry = 2
    while i < retry + 1:
        if nearbyPath:
            res = con.RouterModule_getPeers(path, nearbyPath=nearbyPath)
        else:
            res = con.RouterModule_getPeers(path)

        if res['error'] == 'not_found':
            print('get_peers: node with path {:s} not found, skipping.'
                  .format(formatted_path))
            return []

        elif res['error'] != 'none':
            print('get_peers: failed with error `{:s}` on {:s}, trying again. {:d} tries remaining.'
                  .format(res['error'], formatted_path, retry-i))
        elif res['result'] == 'timeout':
            print('get_peers: timed out on {:s}, trying again. {:d} tries remaining.'
                  .format(formatted_path, retry-i))
        else:
            return res['peers']

        i += 1

    print('get_peers: failed on final try, skipping {:s}'
          .format(formatted_path))
    return []


def get_all_peers(con, path):
    peers = set()
    keys = set()

    res = get_peers(con, path)
    peers.update(res)

    if not res:
        return keys

    last_peer = res[-1]
    checked_paths = set()

    while len(res) > 1:
        last_path = (last_peer.split('.', 1)[1]
                              .rsplit('.', 2)[0])

        if last_path in checked_paths:
            break
        else:
            checked_paths.add(last_path)

        res = get_peers(con, path, last_path)
        if res:
            last_peer = res[-1]
        else:
            break

        peers.update(res)

    for peer in peers:
        key = peer.split('.', 5)[-1]
        keys |= {key}

    return keys


def get_edges_for_peers(edges, peers, node_ip):
    for peer_key in peers:
        peer_ip = key_utils.to_ipv6(peer_key)

        A = max(node_ip, peer_ip)
        B = min(node_ip, peer_ip)

        edge = {'a': A,
                'b': B}

        if A not in edges:
            edges[A] = []

        if not any(edge['b'] == B for edge in edges[A]):
            edges[A].append(edge)


def send_graph(nodes, edges):
    graph = {
        'nodes': [],
        'edges': [edge for sublist in edges.values()
                  for edge in sublist],
    }

    for node in nodes.values():
        graph['nodes'].append({
            'ip':      node['ip'],
            'version': node['version'],
        })

    print('Nodes: {:d}\nEdges: {:d}\n'.format(len(nodes), len(edges)))

    json_graph = json.dumps(graph)
    print('Sending data to {:s}...'.format(URL))

    payload = {'data': json_graph, 'mail': YOUR_EMAIL, 'version': 2}
    r = requests.post(URL, data=payload)

    if r.text == 'OK':
        print('Done!')
    else:
        print('{:s}'.format(r.text))


if __name__ == '__main__':
    main()
