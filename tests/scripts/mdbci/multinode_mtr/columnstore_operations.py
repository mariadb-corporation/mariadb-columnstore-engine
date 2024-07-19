import requests
import json
import re

from mdb_buildbot_scripts.operations import host

REPL_PASS = 'ct570c3521fCCR#'
REPL_USER = 'repl'
MAX_PASS = 'ct570c3521fCCR#ef'
MAX_USER = 'max'
UTIL_PASS = 'ect436c3rf34f5#ct570c3521fCCR'
UTIL_USER = 'util'
IP_KEY = '93816fa66cc2d8c224e62275bd4f248234dd4947b68d4af2b29671dd7d5532dd'


def checkColumnstoreCluster(IP_KEY, N, csVM):
    res = 0
    x = requests.get(f"https://{csVM[0].ip_address}:8640/cmapi/0.4.0/cluster/status",
                     headers={"Content-Type": "application/json", "x-api-key": f"{IP_KEY}"},
                     verify=False)
    j = x.json()
    host.printInfo(json.dumps(j, indent=2))
    # checking data from status JSON:
    # number of nodes
    # master is only one node
    # master is readwrite
    # slaves are readonly
    # number of slaves is N - 1
    try:
        numJ = j['num_nodes']
        host.printInfo(f"Number of nodes after restore: {numJ}")
        if N != int(numJ):
            host.printInfo(f"Wrong number of nodes after upgrade")
            res = 1

        k = list(j.keys())
        ipRegex = re.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$")
        masters = 0
        slaves = 0
        for node in k:
            if ipRegex.match(node):
                if j[node]['dbrm_mode'] == 'slave':
                    host.printInfo(f"node {node} is slave")
                    slaves = slaves + 1
                    if j[node]['cluster_mode'] != 'readonly':
                        res = 1
                        host.printInfo(f"Slave {node} is not readonly")
                if j[node]['dbrm_mode'] == 'master':
                    host.printInfo(f"node {node} is master")
                    masters = masters + 1
                    if j[node]['cluster_mode'] != 'readwrite':
                        res = 1
                        host.printInfo(f"Master {node} is not readwrite")

        if masters != 1:
            res = 1
            host.printInfo(f"Wrong number of masters - {masters}")

        if slaves != N - 1:
            res = 1
            host.printInfo(f"Wrong number of slavess - {slaves}")
    except:
        host.printInfo("Error decoding of cluster status JSON")
        res = 1
    return res
