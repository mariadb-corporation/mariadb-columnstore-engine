import socket
from contextlib import ExitStack, contextmanager
from typing import Dict, List, Optional, Tuple
from unittest.mock import patch

DEFAULT_LOCALHOST_HOSTNAME = 'localhost'
DEFAULT_LOCALHOST_IP = '127.0.0.1'

CUR_HOST_IP = '198.51.100.10'
CUR_HOST_HOSTNAME = 'current-host.test'

class MockResolutionBuilder:
    """Builder for creating complex name/IP resolution mocks as a context manager.
    We'll need all of its abilities later, when we add centralized address management.

    Usage:
        cm = (
            MockResolutionBuilder()
            .add_mapping('nodeA.local', '10.0.0.1')
            .add_mapping('nodeB.local', '10.0.0.2', bidirectional=False)
            .add_rev_mapping('10.0.0.2', 'nodeB.no_reverse')
            .build()
        )
        with cm:
            ...
    """

    def __init__(self):
        # Forward: hostname -> ip
        self._forward: Dict[str, str] = {}
        # Reverse: ip -> (primary_hostname, alias_list)
        self._reverse: Dict[str, Tuple[str, List[str]]] = {}
        # Defaults used when no explicit mapping is provided
        self._default_ip: Optional[str] = None
        self._default_hostname: Optional[str] = None

    def add_mapping(
        self,
        hostname: str,
        ip: str,
        bidirectional: bool = True,
    ):
        """Add a forward mapping hostname->ip.

        - If bidirectional is True (default), also add reverse ip->hostname.
        - If bidirectional is False, reverse resolution for this IP will fail
          unless explicitly set later via add_rev_mapping().
        """
        self._forward[hostname] = ip
        # Initialize defaults on first mapping (can be overwritten by set_default)
        if self._default_ip is None:
            self._default_ip = ip
        if self._default_hostname is None:
            self._default_hostname = hostname
        if bidirectional:
            # Always store reverse as a tuple: (primary, aliases)
            self._reverse[ip] = (hostname, [])
        return self

    def add_rev_mapping(self, ip: str, hostname: str, aliases: Optional[List[str]] = None):
        """Explicitly set reverse resolution mapping for IP -> hostname (and aliases).

        - Ensures reverse resolution succeeds for this IP.
        - Overrides any previous reverse mapping for this IP.
        - If aliases are provided, they will be returned by gethostbyaddr as the alias list.
        """
        self._reverse[ip] = (hostname, aliases or [])
        return self

    def set_default(self, ip: str, hostname: str):
        """Set default ip/hostname when a query is not present in mappings."""
        self._default_ip = ip
        self._default_hostname = hostname
        return self

    def _resolve_forward(self, host: str) -> Tuple[str, str]:
        """Resolve hostname or IP to (ip, hostname) using mappings/defaults."""
        # If input looks like an IP, return it with reverse or default hostname
        try:
            socket.inet_pton(socket.AF_INET, host)
            ip = host
            reverse = self._reverse.get(ip)
            if reverse is not None:
                primary, _aliases = reverse
                hostname = primary
            else:
                hostname = self._default_hostname or host
            return ip, hostname
        except OSError:
            pass

        # Treat as hostname
        if host in self._forward:
            ip = self._forward[host]
            hostname = host
            return ip, hostname

        if self._default_ip and self._default_hostname:
            return self._default_ip, self._default_hostname

        # As a last resort, echo back (host, host)
        return host, host

    def build(self):

        def _fake_getaddrinfo(host, port, family=socket.AF_UNSPEC, type=0, proto=0, flags=0):
            # Only handle AF_INET calls; otherwise, simulate failure
            if family not in (socket.AF_UNSPEC, socket.AF_INET):
                raise socket.gaierror
            # For localhost, return loopback first and include CUR_HOST_IP as secondary
            if host == DEFAULT_LOCALHOST_HOSTNAME:
                return [
                    (socket.AF_INET, socket.SOCK_STREAM, 6, '', (DEFAULT_LOCALHOST_IP, port)),
                    (socket.AF_INET, socket.SOCK_STREAM, 6, '', (CUR_HOST_IP, port)),
                ]
            ip, _ = self._resolve_forward(host)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (ip, port))]

        def _fake_gethostbyname(name: str) -> str:
            ip, _ = self._resolve_forward(name)
            return ip

        def _fake_gethostbyaddr(addr: str):
            # If no reverse record was set, simulate reverse lookup failure
            if addr not in self._reverse:
                raise socket.herror
            primary, aliases = self._reverse[addr]
            return (primary, aliases, [addr])

        @contextmanager
        def _ctx():
            patches = [
                # Patch socket-level resolvers (NetworkManager uses these under the hood)
                patch('socket.getaddrinfo', side_effect=_fake_getaddrinfo),
                patch('socket.gethostbyname', side_effect=_fake_gethostbyname),
                patch('socket.gethostbyaddr', side_effect=_fake_gethostbyaddr),
                # Patch local identity to be synthetic; avoid real system calls
                patch('socket.gethostname', return_value=CUR_HOST_HOSTNAME),
                patch('socket.getfqdn', return_value=CUR_HOST_HOSTNAME),
            ]
            with ExitStack() as stack:
                for p in patches:
                    stack.enter_context(p)
                yield

        return _ctx()


def simple_resolution_mock(hostname: str, ip: str):
    """Return a context manager for simple name/IP resolution mocking.

    Behavior:
    - Unknown hostnames default to the synthetic current-host identity: (CUR_HOST_IP, CUR_HOST_HOSTNAME).
    - "localhost" resolves to 127.0.0.1, with reverse lookup to "localhost".
    - The synthetic current host name resolves to CUR_HOST_IP, with reverse lookup back to CUR_HOST_HOSTNAME.
    - The provided (hostname, ip) mapping is added bidirectionally.
    - socket.gethostname() / getfqdn() are patched to CUR_HOST_HOSTNAME to avoid touching the real system.
    """
    return (
        make_local_resolution_builder()
        .add_mapping(hostname, ip, bidirectional=True)
        .build()
    )


def make_local_resolution_builder() -> MockResolutionBuilder:
    """Return a MockResolutionBuilder pre-populated with local/current host
    resolution mappings. Tests can add additional mappings onto this builder
    before calling .build() to obtain a context manager.
    """
    return (
        MockResolutionBuilder()
        .set_default(CUR_HOST_IP, CUR_HOST_HOSTNAME)
        # Localhost forward and reverse
        .add_mapping(DEFAULT_LOCALHOST_HOSTNAME, DEFAULT_LOCALHOST_IP)
        .add_rev_mapping(DEFAULT_LOCALHOST_IP, DEFAULT_LOCALHOST_HOSTNAME)
        # Synthetic current host forward and reverse
        .add_mapping(CUR_HOST_HOSTNAME, CUR_HOST_IP)
        .add_rev_mapping(CUR_HOST_IP, CUR_HOST_HOSTNAME, [DEFAULT_LOCALHOST_HOSTNAME])
    )