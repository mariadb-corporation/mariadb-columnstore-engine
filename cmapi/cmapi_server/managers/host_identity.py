"""
This module implements hostname <-> IP addr resolution logic (it is not as simple as it may look).

Most of CMAPI/Columnstore code assumes that each node has 1 IP address and 1 hostname.
But in reality it can have N IP addresses and M hostnames, and we need some way to choose which IP/hostname will be primary.
Also some of these names will not be visible from the other hosts (like local aliases from systemd).
And some sources are more reliable than the others (DNS > /etc/hosts), so we must order them by reliablity and choose the most reliable.

So:
1. We must choose 1 IP address and 0/1 hostnames as primary (of many)
2. We need to filter out unreliable names
3. We must order IPs/hostnames by source reliability
4. There can be very many resolving sources, so we cannot resolve everything ourselves, and must rely on OS resolving
  (see /etc/nsswitch.conf, there are local /etc/hosts, DNS, mDNS, systemd-resolved, LDAP, myhostname, etc)
5. But most important sources (DNS and /etc/hosts, recommended by our manual...) must be checked and ordered by our policy
"""
import hashlib
import ipaddress
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from typing import Optional, Union

import dns.resolver
import dns.reversename

from cmapi_server.exceptions import CMAPIBasicError, ResolutionError, ResolutionPolicyViolationError
from cmapi_server.managers.network import NetworkManager
from cmapi_server.managers.resolving_sources import get_resolving_source

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

logger = logging.getLogger(__name__)


@dataclass
class HostIdentity:
    """Host's network identity, IP addrs and hostnames that are visible from other hosts"""
    input: str
    # The first, most important IP addr and host name (ordering is done by policy)
    primary_ip: str
    primary_name: Optional[str]  # Name can be missing (but policy can make it required)
    ips: list[str]  # All IP addrs
    names: list[str]  # All host names (only those that are visible to other hosts)
    unique_key: str  # unique id of this host, will be used later for aliases
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @staticmethod
    def from_policy(input: str, policy: 'ResolutionPolicy', ips: Sequence[IPAddress], names: Sequence[str]) -> 'HostIdentity':
        """Construct a HostIdentity from filtered addresses and names using policy ordering."""
        if not ips:
            raise ResolutionPolicyViolationError('All resolved addresses were rejected by policy.')

        ordered_ips = policy.order_addresses(ips)
        # Calculate host unique key from its addresses (we prefer globals as more stable ones)
        globals_ordered = [ip for ip in ordered_ips if ip.is_global]
        privates_ordered = [ip for ip in ordered_ips if ip.is_private]
        to_hash = globals_ordered if globals_ordered else privates_ordered
        hasher = hashlib.sha256()
        for ip in to_hash:
            hasher.update(str(ip).encode('utf-8'))
        unique_key = hasher.hexdigest()

        return HostIdentity(
            input=input,
            ips=[str(ip) for ip in ordered_ips],
            names=list(names),
            primary_ip=str(ordered_ips[0]),
            primary_name=names[0] if names else None,
            unique_key=unique_key,
        )

    @property
    def effective_hostname(self) -> str:
        """Hostname or primary IP if hostname is not set

        If policy requires host to have a hostname and it doesn't, we won't even get here
        """
        return self.primary_name or self.primary_ip

    def __repr__(self) -> str:
        parts: list[str] = [
            f'input={self.input!r}',
            f'primary_ip={self.primary_ip!r}',
        ]
        if self.primary_name is not None:
            parts.append(f'primary_name={self.primary_name!r}')
        # Don't show addrs and hostnames if there is only one addr or hostname
        if self.ips != [self.primary_ip]:
            parts.append(f'ips={self.ips!r}')
        if self.names != [self.primary_name]:
            parts.append(f'names={self.names!r}')
        parts.append(f'unique_key={self.unique_key[:4]}')
        parts.append(f'observed_at={self.observed_at.replace(microsecond=0).isoformat()!r}')
        return 'HostIdentity(' + ', '.join(parts) + ')'

    def __str__(self) -> str:
        return repr(self)


@dataclass(frozen=True)
class ResolutionPolicy:
    """Class to represent our requirements for the addresses, how are they resolved,
      what must be filtered out, how addresses are ordered, etc.

    It's nice to separate these concerns from the resolving itself.
    Also maybe in the future we'll make it configurable by users.
    """

    allow_private_ips: bool = True
    allow_ipv6: bool = False
    require_hostname: bool = False

    def filter_addresses(self, addrs: Iterable[str]) -> list[IPAddress]:
        """Filter out IP addresses that don't match the policy."""
        ips: list[IPAddress] = []
        logger.debug(
            'Filtering addresses: %s (allow_private_ips=%s, allow_ipv6=%s)',
            list(addrs), self.allow_private_ips, self.allow_ipv6
        )
        for addr in addrs:
            ip = _ip_or_none(addr)
            if ip is None:
                logger.error('Skip %s: not a valid IP literal', addr)
                continue

            # Fixed rejections first
            if ip.is_loopback:
                logger.debug('Skip %s: loopback address', addr)
                continue
            if ip.is_link_local:
                logger.debug('Skip %s: link-local address', addr)
                continue
            if ip.is_multicast:
                logger.debug('Skip %s: multicast address', addr)
                continue
            if isinstance(ip, ipaddress.IPv4Address) and int(ip) == int(ipaddress.IPv4Address('255.255.255.255')):
                logger.debug('Skip %s: IPv4 broadcast address', addr)
                continue

            # Policy conditionals
            if not self.allow_ipv6 and isinstance(ip, ipaddress.IPv6Address):
                logger.debug('Skip %s: IPv6 not allowed by policy', addr)
                continue
            if not ip.is_global and not ip.is_private:
                logger.debug('Skip %s: neither global nor private address', addr)
                continue
            if ip.is_private and not self.allow_private_ips:
                logger.debug('Skip %s: private address but private use disabled', addr)
                continue

            ips.append(ip)
            logger.debug('Accept %s', addr)
        return ips

    def order_addresses(self, ips: Sequence[IPAddress]) -> list[IPAddress]:
        """Order IPs deterministically to choose the primary IP."""
        def key(ip: IPAddress) -> tuple[int, int, int, int, int, int, int, int]:
            is_global = 0 if ip.is_global else 1
            is_ipv4 = 0 if isinstance(ip, ipaddress.IPv4Address) else 1
            return (
                is_global,
                is_ipv4,
                *list(ip.packed)
            )
        return sorted(ips, key=key)


class HostAddressManager:
    """Calculates HostIdentity from passed hostname or IP address."""

    def __init__(self, policy: Optional[ResolutionPolicy] = None) -> None:
        self._policy = policy if policy is not None else ResolutionPolicy()
        self._cache: dict[str, HostIdentity] = {}

    def get_identity(self, target: str) -> HostIdentity:
        """Resolve and normalize a hostname or IP."""
        if target in self._cache:
            return self._cache[target]

        target = target.strip()

        ip = _ip_or_none(target)
        if ip is not None:
            identity = self._get_identity_from_ip(ip, target)
        else:
            identity = self._get_identity_from_hostname(target)

        self._cache[target] = identity
        return identity

    def get_local_identity(self) -> HostIdentity:
        """Calculate HostIdentity for the current host."""
        try:
            local_ips = NetworkManager.get_current_node_ips(
                ignore_loopback=False,
                only_ipv4=not self._policy.allow_ipv6,
            )
        except:
            logger.exception('Failed to get local IP addresses')
            raise

        # Use the first IP that resolves into a normal identity that is not rejected by policy
        for ip_text in set(local_ips):
            try:
                return self.get_identity(ip_text)
            except CMAPIBasicError:
                logger.debug('Local identity candidate %s failed resolution', ip_text)
                continue
        raise ResolutionPolicyViolationError('Could not determine any acceptable local IP addresses under current policy.')

    def check_hostname_rev_lookup(self, hostname: str) -> tuple[HostIdentity, bool]:
        """Resolve hostname and check that at least one of its IPs resolves back to it.

        Returns identity and boolean, true means that roundtrip check was successful.
        """
        identity = self.get_identity(hostname)

        normalized = hostname.strip().lower()
        if not normalized:
            return identity, False

        for ip_text in identity.ips:
            ip = _ip_or_none(ip_text)
            if ip is None:
                logger.error('Invalid IP address: %s', ip_text)
                continue

            try:
                reverse_names = self._get_names_of_ip(ip)
            except Exception:
                logger.exception('Failed to get reverse names for %s', ip)
                continue

            for rev_name in reverse_names:
                # Count reverse names like "mcs1.<domain>" as a match for "mcs"
                # It is true because of search_domain/ndots (in /etc/resolv.conf)
                # Resolvers respect these options and will add them to the hostname
                if rev_name == normalized or rev_name.startswith(normalized + '.'):
                    logger.debug('Roundtrip check passed for %s: %s', hostname, ip)
                    return identity, True

        logger.warning('Roundtrip check failed for %s', hostname)
        return identity, False

    def _get_identity_from_ip(self, ip: IPAddress, original_input: str) -> HostIdentity:
        if not self._policy.filter_addresses([str(ip)]):
            raise ResolutionPolicyViolationError('Input IP address was rejected by policy')

        names: list[str] = self._get_names_of_ip(ip)
        if self._policy.require_hostname and not names:
            logger.warning('Reject %s: no names found for this IP and policy requires hostname', original_input)
            raise ResolutionPolicyViolationError('Policy requires a hostname for the input IP address (no PTR record found).')

        return HostIdentity.from_policy(original_input, self._policy, [ip], sorted(names))

    def _get_identity_from_hostname(self, hostname: str) -> HostIdentity:
        normalized = hostname.strip().lower()
        if _is_fqdn(normalized):
            return self._get_identity_from_fqdn(normalized)
        else:
            return self._get_identity_from_non_fqdn(hostname)

    def _get_identity_from_fqdn(self, fqdn: str) -> HostIdentity:
        # Get IPs from hostname (via DNS), filter them, then get names from each IP
        addrs = self._resolve_dns(fqdn)
        if not addrs:
            raise ResolutionError(f'Could not resolve {fqdn} to any IP addresses.')
        filtered_ips = self._policy.filter_addresses([str(ip) for ip in addrs])

        # Resolve each IP back to names and check if there is any that resolved back to passed fqdn
        names: set[str] = {fqdn}
        roundtrip_found = False
        for ip in filtered_ips:
            names_of_ip = self._get_names_of_ip(ip)
            if fqdn in names_of_ip:
                roundtrip_found = True
            names.update(names_of_ip)

        if not roundtrip_found:
            logger.warning(
                'FQDN %s failed DNS forward/reverse round-trip; IPs: %s',
                fqdn,
                [str(ip) for ip in filtered_ips],
            )
            raise ResolutionError(f'{fqdn} failed the DNS round-trip check — forward and reverse records do not match.')

        if not filtered_ips:
            raise ResolutionPolicyViolationError('All resolved addresses were rejected by policy.')

        return HostIdentity.from_policy(fqdn, self._policy, filtered_ips, sorted(names))

    def _get_identity_from_non_fqdn(self, hostname: str) -> HostIdentity:
        # Like FQDN version, but we know that nothing will resolve back to passed hostname
        candidate_ips: set[str] = set(
            NetworkManager.resolve_hostname_to_ips(
                hostname,
                only_ipv4=not self._policy.allow_ipv6,
                exclude_loopback=False,
            )
        )

        ips = self._policy.filter_addresses(candidate_ips)
        if not ips:
            raise ResolutionPolicyViolationError('All resolved addresses were rejected by policy.')

        # Collect names of IPs
        names: set[str] = set()
        for ip_text in list(candidate_ips):
            ip = _ip_or_none(ip_text)
            if ip is None:
                logger.error('Invalid IP address: %s', ip_text)
                continue
            names.update(self._get_names_of_ip(ip))

        if self._policy.require_hostname and not names:
            logger.error(
                'Non-FQDN name %s does not resolve to any valid hostname; candidate IPs: %s',
                hostname,
                sorted(candidate_ips),
            )
            raise ResolutionPolicyViolationError('Policy requires a hostname for the input host, but DNS did not return any FQDN names.')

        return HostIdentity.from_policy(hostname, self._policy, ips, sorted(names))

    def _resolve_dns(self, hostname: str) -> list[IPAddress]:
        """Resolve the given hostname using DNS and return addresses."""
        resolver = get_resolving_source('dns')
        return resolver.resolve(hostname)

    def _get_names_of_ip(self, ip: IPAddress) -> list[str]:
        """Fetch PTR names for an IP via DNS."""
        try:
            resolver = get_resolving_source('dns')
            return resolver.reverse(ip)
        except Exception:
            logger.exception('ip-to-name lookup unexpected failure for %s', ip)
            raise

    # DNS abstraction methods for easier mocking
    def _dns_resolve_ipv4(self, hostname: str) -> list[str]:
        resolver = dns.resolver.Resolver(configure=True)
        results: list[str] = []
        for rdata in resolver.resolve(hostname, 'A', raise_on_no_answer=False):
            try:
                results.append(rdata.to_text())
            except Exception:
                continue
        return results

    def _dns_resolve_ipv6(self, hostname: str) -> list[str]:
        resolver = dns.resolver.Resolver(configure=True)
        results: list[str] = []
        for rdata in resolver.resolve(hostname, 'AAAA', raise_on_no_answer=False):
            try:
                results.append(rdata.to_text())
            except Exception:
                continue
        return results

    def _dns_reverse(self, ip_text: str) -> list[str]:
        reverse_name = dns.reversename.from_address(ip_text)
        answer = dns.resolver.resolve(reverse_name, 'PTR', raise_on_no_answer=False)
        names: list[str] = []
        for ptr_rdata in answer:
            try:
                name = str(ptr_rdata.target).rstrip('.').lower()
                if name:
                    names.append(name)
            except Exception:
                continue
        return names

    def _contains_private(self, addrs: list[str]) -> bool:
        """Return True if any resolvable address string is a private IP."""
        for addr in addrs:
            ip = _ip_or_none(addr)
            if ip is None:
                continue
            if ip.is_private:
                return True
        return False


@lru_cache(maxsize=1)  # singleton
def get_host_address_manager() -> 'HostAddressManager':
    return HostAddressManager()


def _ip_or_none(val: str) -> Optional[IPAddress]:
    try:
        return ipaddress.ip_address(val)
    except ValueError:
        return None

def _is_ip_address(val: str) -> bool:
    return _ip_or_none(val) is not None

def _is_fqdn(name: str) -> bool:
    """Return True if the string is a valid FQDN (lower-cased, no trailing dot).

    Rules for labels (per common DNS practice):
    - Each label is 1..63 characters.
    - Only ASCII letters, digits, and hyphen are allowed (LDH rule).
    - A label cannot start or end with a hyphen.
    - The name must contain at least one dot separating labels.
    """
    if not name:
        return False
    if name.endswith('.'):
        return False
    if '.' not in name:
        return False
    labels = name.split('.')
    for label in labels:
        if not label or len(label) > 63:
            return False
        for ch in label:
            if not (ch.isalnum() or ch == '-'):
                return False
        if label[0] == '-' or label[-1] == '-':
            return False
    return True
