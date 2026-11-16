#!/usr/bin/env python3
"""Enable / disable the FIRERPA device-side proxy that feeds mitmdump.

    python setup_mitm_proxy.py on   [--package tv.danmaku.bili]
    python setup_mitm_proxy.py off

Requires the lamda client from the cloned firerpa/lamda repository and a
mitmdump listening on --port (see grpc_mitm_addon.py).
"""

from __future__ import annotations

import argparse
import os
import sys

from lamda.client import Device, GproxyProfile, GproxyType

DEVICE_HOST = os.environ.get("LAMDA_HOST", "192.168.42.238")
DEVICE_PORT = int(os.environ.get("LAMDA_PORT", "65000"))
PROXY_HOST = os.environ.get("MITM_HOST", "192.168.1.201")
PROXY_PORT = int(os.environ.get("MITM_PORT", "18081"))
CA_PATH = os.environ.get(
    "MITM_CA", os.path.expanduser("~/.mitmproxy/mitmproxy-ca-cert.pem")
)


def enable(device: Device, package: str | None) -> None:
    if not os.path.exists(CA_PATH):
        raise SystemExit(f"mitmproxy CA not found: {CA_PATH}")
    print(f"install ca  : {CA_PATH}")
    print(device.install_ca_certificate(CA_PATH))

    profile = GproxyProfile()
    profile.type = GproxyType.HTTP_CONNECT
    profile.host = PROXY_HOST
    profile.port = PROXY_PORT
    profile.bypass_local_subnet = True
    profile.udp_proxy = False
    profile.interface = "auto"
    if package:
        profile.application.set(device.application(package))
        print(f"scope       : {package}")
    else:
        print("scope       : all apps")
    print(f"proxy       : http_connect {PROXY_HOST}:{PROXY_PORT}")
    print(device.start_gproxy(profile))
    print(f"running     : {device.is_gproxy_running()}")


def disable(device: Device) -> None:
    print(f"running before: {device.is_gproxy_running()}")
    try:
        print(device.stop_gproxy())
    except Exception as error:  # noqa: BLE001 - report whatever the server says
        print(f"stop_gproxy failed: {error}")
    print(f"running after : {device.is_gproxy_running()}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("on", "off", "status"))
    parser.add_argument("--package", default="tv.danmaku.bili")
    parser.add_argument("--host", default=DEVICE_HOST)
    parser.add_argument("--port", type=int, default=DEVICE_PORT)
    args = parser.parse_args(argv)

    device = Device(args.host, port=args.port)
    if args.action == "on":
        enable(device, args.package or None)
    elif args.action == "off":
        disable(device)
    else:
        print(f"gproxy running: {device.is_gproxy_running()}")
        print(f"ca installed  : {device.is_ca_certificate_installed(CA_PATH)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
