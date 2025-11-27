import ipaddress
import logging
from functools import cache
from typing import Union

import dns.resolver
import dns.reversename

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

logger = logging.getLogger(__name__)


class ResolvingSource:
    """Base class for name/IP resolution sources"""

    def resolve(self, hostname: str) -> list[IPAddress]:
        """Forward lookup: hostname -> list of IPAddress objects."""
        raise NotImplementedError

    def reverse(self, ip: IPAddress) -> list[str]:
        """Reverse lookup: IPAddress -> list of normalized hostnames."""
        raise NotImplementedError


class DNSResolvingSource(ResolvingSource):
    """DNS-based resolving source"""

    def resolve(self, hostname: str) -> list[IPAddress]:
        resolver = dns.resolver.Resolver(configure=True)
        results: list[IPAddress] = []
        seen: set[str] = set()

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

    def reverse(self, ip: IPAddress) -> list[str]:
        ip_text = str(ip)
        reverse_name = dns.reversename.from_address(ip_text)
        try:
            answer = dns.resolver.resolve(reverse_name, 'PTR', raise_on_no_answer=False)
        except dns.resolver.NoAnswer:
            return []

        names: list[str] = []
        for ptr_rdata in answer:
            try:
                name = str(ptr_rdata.target).rstrip('.').lower()
            except Exception:
                continue
            if name:
                names.append(name)
        return names


@cache
def get_resolving_source(name: str) -> ResolvingSource:
    if name == 'dns':
        return DNSResolvingSource()
    raise ValueError(f'Unknown resolving source: {name}')
