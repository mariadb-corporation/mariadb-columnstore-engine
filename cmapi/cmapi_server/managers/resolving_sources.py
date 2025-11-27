import ipaddress
import logging
from enum import Enum
from functools import cache
from typing import Dict, List, Set, Type, Union

import dns.resolver
import dns.reversename

from cmapi_server.managers.network import NetworkManager

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

logger = logging.getLogger(__name__)


class ResolvingSourceName(str, Enum):
    DNS = 'dns'
    OS = 'os'


class ResolvingSource:
    """Base class for name/IP resolution sources"""

    name: ResolvingSourceName

    def resolve(self, hostname: str) -> List[IPAddress]:
        """Forward lookup: hostname -> list of IPAddress objects."""
        raise NotImplementedError

    def reverse(self, ip: IPAddress) -> List[str]:
        """Reverse lookup: IPAddress -> list of normalized hostnames."""
        raise NotImplementedError


class DNSResolvingSource(ResolvingSource):
    """Use only DNS for resolution"""

    name = ResolvingSourceName.DNS

    def resolve(self, hostname: str) -> List[IPAddress]:
        resolver = dns.resolver.Resolver(configure=True)
        results: List[IPAddress] = []
        seen: Set[str] = set()

        # A records
        try:
            answer = resolver.resolve(hostname, 'A', raise_on_no_answer=False)
            for rdata in answer:
                try:
                    ip_text = rdata.to_text()
                    if ip_text in seen:
                        continue
                    ip = ipaddress.ip_address(ip_text)
                except Exception:
                    continue
                seen.add(ip_text)
                results.append(ip)
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            pass

        # AAAA records
        try:
            answer = resolver.resolve(hostname, 'AAAA', raise_on_no_answer=False)
            for rdata in answer:
                try:
                    ip_text = rdata.to_text()
                    if ip_text in seen:
                        continue
                    ip = ipaddress.ip_address(ip_text)
                except Exception:
                    continue
                seen.add(ip_text)
                results.append(ip)
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            pass

        if not results:
            logger.warning('DNS lookups (A/AAAA) did not return any addresses for %s', hostname)

        return results

    def reverse(self, ip: IPAddress) -> List[str]:
        ip_text = str(ip)
        reverse_name = dns.reversename.from_address(ip_text)
        try:
            answer = dns.resolver.resolve(reverse_name, 'PTR', raise_on_no_answer=False)
        except dns.resolver.NoAnswer:
            return []

        names: List[str] = []
        for ptr_rdata in answer:
            try:
                name = str(ptr_rdata.target).rstrip('.').lower()
            except Exception:
                continue
            if name:
                names.append(name)
        return names


class OSResolvingSource(ResolvingSource):
    """Uses all the sources defined in /etc/nsswitch.conf for resolving"""

    name = ResolvingSourceName.OS

    def resolve(self, hostname: str) -> List[IPAddress]:
        ip_texts = NetworkManager.resolve_hostname_to_ips(
            hostname,
            only_ipv4=False,
            exclude_loopback=False,
        )

        results: List[IPAddress] = []
        seen: set[str] = set()
        for ip_text in ip_texts:
            if ip_text in seen:
                continue
            try:
                ip = ipaddress.ip_address(ip_text)
            except ValueError:
                logger.error('OS resolver returned invalid IP address %s for host name %s, skipping', ip_text, hostname)
                continue
            seen.add(ip_text)
            results.append(ip)
        return results

    def reverse(self, ip: IPAddress) -> List[str]:
        return NetworkManager.get_hostnames_by_ip(str(ip))


_RESOLVER_REGISTRY: Dict[ResolvingSourceName, Type[ResolvingSource]] = {
    cls.name: cls for cls in (DNSResolvingSource, OSResolvingSource)
}


@cache
def get_resolving_source(name: ResolvingSourceName) -> ResolvingSource:
    cls = _RESOLVER_REGISTRY.get(name)
    if cls is None:
        raise ValueError(f'Unknown resolving source: {name}')
    return cls()
