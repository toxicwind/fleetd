#!/usr/bin/env python3
"""Add /fleet-ws to the Tailscale funnel serve config via LocalAPI.

Reads the live config, backs it up, adds ONLY our handler, writes it
back, then verifies. Existing handlers are never touched.
Pattern borrowed from the /exec-ws and /whatsapp-webhook route adds.
"""
import http.client
import json
import socket
import sys

SOCK = "/var/run/tailscale/tailscaled.sock"
HOST_KEY = "github-mcp-host.tailc9ac71.ts.net:443"
BACKUP = "/home/toxic/fleet/fleetd/serve-config.backup.json"
ROUTE = "/fleet-ws"
TARGET = "http://127.0.0.1:25151/fleet-ws"


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost")
        self._path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self._path)


def api(method, url, body=None):
    c = UnixHTTP(SOCK)
    headers = {"Host": "local-tailscaled.sock"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    c.request(method, url, body=data, headers=headers)
    r = c.getresponse()
    return r.status, r.read()


def main():
    st, raw = api("GET", "/localapi/v0/serve-config")
    print("GET -> %d, %d bytes" % (st, len(raw)), flush=True)
    if st != 200 or not raw:
        print("GET failed, aborting", flush=True)
        return 1
    cfg = json.loads(raw)
    with open(BACKUP, "w") as f:
        json.dump(cfg, f, indent=2)
    handlers = cfg["Web"][HOST_KEY]["Handlers"]
    print("current handlers: " + str(sorted(handlers)), flush=True)
    if handlers.get(ROUTE) == {"Proxy": TARGET}:
        print("route already present, nothing to do", flush=True)
        return 0
    handlers[ROUTE] = {"Proxy": TARGET}
    for method in ("POST", "PUT"):
        st, out = api(method, "/localapi/v0/serve-config", cfg)
        print("%s -> %d %r" % (method, st, out[:200]), flush=True)
        if st in (200, 204):
            break
    else:
        print("both methods failed, aborting", flush=True)
        return 1
    st, raw = api("GET", "/localapi/v0/serve-config")
    h2 = cfg["Web"][HOST_KEY]["Handlers"]
    cfg2 = json.loads(raw)
    h2 = sorted(cfg2["Web"][HOST_KEY]["Handlers"])
    print("handlers now: " + str(h2), flush=True)
    if ROUTE not in h2:
        print("ROUTE NOT PRESENT", flush=True)
        return 1
    print("fleet-ws -> " + cfg2["Web"][HOST_KEY]["Handlers"][ROUTE]["Proxy"], flush=True)
    print("OK", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
