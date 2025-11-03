import hashlib
import ipaddress
import logging
import socket
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Optional, Union

import dns.resolver
import dns.reversename

from cmapi_server.exceptions import CMAPIBasicError, ResolutionError, ResolutionPolicyViolationError
from cmapi_server.managers.network import NetworkManager

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)  # singleton
def get_host_address_manager() -> 'HostAddressManager':
    return HostAddressManager()


@dataclass(frozen=True)
class ResolutionPolicy:
    """Rules controlling how hosts are resolved, what is/isn't allowed (like IPv6)."""

    allow_private_ips: bool = True
    allow_ipv6: bool = False
    require_hostname: bool = False

    def validate_hostname(self, name: str) -> str:
        """Validate and normalize a hostname to FQDN."""
        normalized = name.strip().lower()
        if not _is_fqdn(normalized):
            raise ResolutionPolicyViolationError(f'The name {name} is not a fully qualified domain name.')
        return normalized

    def filter_addresses(self, addrs: Iterable[str]) -> list[IPAddress]:
        """Filter out IP addresses that don't match the policy."""
        ips: list[IPAddress] = []
        logger.debug(
            'Filtering addresses: %s (allow_private_ips=%s, allow_ipv6=%s)',
            list(addrs), self.allow_private_ips, self.allow_ipv6
        )
        for addr in addrs:
            try:
                ip = ipaddress.ip_address(addr)
            except ValueError:
                logger.debug('Skip %s: not a valid IP literal', addr)
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
        """Deterministic ordering: global first, then IPv4 before IPv6, then sort by numeric value."""
        def key(ip: IPAddress) -> tuple[int, int, int, int, int, int, int, int]:
            is_global = 0 if ip.is_global else 1
            is_ipv4 = 0 if isinstance(ip, ipaddress.IPv4Address) else 1
            return (
                is_global,
                is_ipv4,
                *list(ip.packed)
            )
        return sorted(ips, key=key)


@dataclass
class HostIdentity:
    """Normalized result of host resolution (after policy is enforced)"""
    input: str
    addresses: list[str]
    names: list[str]
    primary_ip: str
    primary_name: Optional[str]
    unique_key: str  # unique id of this host, will be used later for aliases
    observed_at: float = field(default_factory=lambda: time.time())

    @staticmethod
    def from_policy(input: str, policy: ResolutionPolicy, ips: Sequence[IPAddress], names: Sequence[str]) -> 'HostIdentity':
        """Construct a HostIdentity from filtered addresses and names using policy ordering and hashing rules."""
        if not ips:
            raise ResolutionPolicyViolationError('All resolved addresses were rejected by policy (loopback / link-local / multicast).')
        ordered = policy.order_addresses(ips)
        addresses = [str(ip) for ip in ordered]

        # Calculate host unique key from its addresses (we prefer globals as more stable ones)
        globals_sorted = [str(ip) for ip in ordered if ip.is_global]
        privates_sorted = [str(ip) for ip in ordered if ip.is_private]
        basis = globals_sorted if globals_sorted else privates_sorted
        hasher = hashlib.sha256()
        for address_text in basis:
            hasher.update(address_text.encode('utf-8'))
        unique_key = hasher.hexdigest()

        primary_ip = addresses[0]
        primary_name = names[0] if names else None
        return HostIdentity(
            input=input,
            addresses=addresses,
            names=list(names),
            primary_ip=primary_ip,
            primary_name=primary_name,
            unique_key=unique_key,
        )


class HostAddressManager:
    """In-memory resolver that performs DNS-only lookups, enforces policy, and caches results."""

    def __init__(self, policy: Optional[ResolutionPolicy] = None) -> None:
        self._policy = policy if policy is not None else ResolutionPolicy()
        self._cache: dict[str, HostIdentity] = {}

    @property
    def policy(self) -> ResolutionPolicy:
        return self._policy

    def get_identity(self, target: str) -> HostIdentity:
        """Resolve and normalize a hostname or IP under the current policy."""
        if target in self._cache:
            return self._cache[target]

        target = target.strip()
        # If target is IP literal
        try:
            literal_ip = ipaddress.ip_address(target)
        except ValueError:
            literal_ip = None

        if literal_ip is not None:
            # Use the literal IP only; names via PTR if available
            candidate_ips: set[str] = {str(literal_ip)}
            names_set: set[str] = set(self._reverse_dns_names(literal_ip))
            ips = self._policy.filter_addresses(candidate_ips)
            if self._policy.require_hostname and not names_set:
                logger.debug('Reject %s: no PTR hostname found and policy requires hostname', target)
                raise ResolutionPolicyViolationError('Policy requires a hostname for the input IP address (no PTR record found).')
            if not ips:
                raise ResolutionPolicyViolationError('Input IP address was rejected by policy')
            identity = HostIdentity.from_policy(target, self._policy, ips, sorted(names_set))
            self._cache[target] = identity
            return identity

        # Target is hostname: if it's a valid FQDN, use DNS path; otherwise treat as local alias (e.g., localhost)
        try:
            fqdn = self._policy.validate_hostname(target)
        except ResolutionPolicyViolationError:
            fqdn = None
        if fqdn is None:
            # Non-FQDN (e.g., localhost). Seed with system resolver only.
            candidate_ips: set[str] = set(
                NetworkManager.resolve_hostname_to_ips(
                    target,
                    only_ipv4=not self._policy.allow_ipv6,
                    exclude_loopback=False,
                )
            )
            # Collect names by PTR of candidate IPs and include original token
            names_set: set[str] = {target}
            for ip_text in list(candidate_ips):
                try:
                    ip_obj = ipaddress.ip_address(ip_text)
                except ValueError:
                    continue
                names_set.update(self._reverse_dns_names(ip_obj))
            ips = self._policy.filter_addresses(candidate_ips)
            if not ips:
                raise ResolutionPolicyViolationError('All resolved addresses were rejected by policy.')
            identity = HostIdentity.from_policy(target, self._policy, ips, sorted(names_set))
            self._cache[target] = identity
            return identity

        # FQDN path: forward DNS, then PTR of accepted IPs; no further expansion
        addrs, _ = self._resolve_dns(fqdn)
        if not addrs:
            raise ResolutionError(f'Could not resolve {fqdn} to any IP addresses.')
        ips = self._policy.filter_addresses([str(ip) for ip in addrs])
        names_set: set[str] = {fqdn}
        ptr_match = False
        for ip in ips:
            ptrs = self._reverse_dns_names(ip)
            if fqdn in ptrs:
                ptr_match = True
            names_set.update(ptrs)
        if not ptr_match:
            raise ResolutionError(f'{fqdn} failed the DNS round-trip check — forward and reverse records do not match.')
        if not ips:
            raise ResolutionPolicyViolationError('All resolved addresses were rejected by policy.')
        identity = HostIdentity.from_policy(fqdn, self._policy, ips, sorted(names_set))
        self._cache[target] = identity
        return identity

    def get_local_identity(self) -> HostIdentity:
        """Return normalized identity of the current host."""
        name = socket.getfqdn().strip().lower()
        candidates: list[str] = []
        if _is_fqdn(name):
            candidates.append(name)
        try:
            local_ips = NetworkManager.get_current_node_ips(
                ignore_loopback=False,
                only_ipv4=not self._policy.allow_ipv6,
            )
        except CMAPIBasicError:
            logger.exception('Failed to get local IP addresses for identity resolution')
            local_ips = []
        candidates.extend(local_ips)
        seen: set[str] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            try:
                return self.get_identity(candidate)
            except CMAPIBasicError:
                logger.exception('Local identity candidate failed resolution: %s', candidate)
                continue
        raise ResolutionPolicyViolationError('Could not determine any acceptable local IP addresses under current policy.')

    def _resolve_dns(self, hostname: str) -> tuple[list[IPAddress], list[str]]:
        """Resolve the given hostname using DNS and return (addresses, names)."""
        resolver = dns.resolver.Resolver(configure=True)

        # IPv4
        a_records: list[IPAddress] = []
        try:
            for a_rdata in resolver.resolve(hostname, 'A', raise_on_no_answer=False):
                a_records.append(ipaddress.ip_address(a_rdata.to_text()))
        except Exception:
            logger.exception('A lookup failed for %s', hostname)

        # IPv6
        aaaa_records: list[IPAddress] = []
        if self._policy.allow_ipv6:
            try:
                for aaaa_rdata in resolver.resolve(hostname, 'AAAA', raise_on_no_answer=False):
                    aaaa_records.append(ipaddress.ip_address(aaaa_rdata.to_text()))
            except Exception:
                logger.exception('AAAA lookup failed for %s', hostname)

        names = [hostname]
        return a_records + aaaa_records, names

    def _reverse_dns_names(self, ip: IPAddress) -> list[str]:
        """Fetch PTR names for an IP via DNS."""
        try:
            reverse_name = dns.reversename.from_address(str(ip))
            answer = dns.resolver.resolve(reverse_name, 'PTR', raise_on_no_answer=False)
            names: list[str] = []
            for ptr_rdata in answer:
                fqdn_name = str(ptr_rdata.target).rstrip('.').lower()
                if _is_fqdn(fqdn_name):
                    names.append(fqdn_name)
            return names
        except Exception:
            logger.exception('PTR lookup failed for %s', ip)
            return []

    def _contains_private(self, addrs: list[str]) -> bool:
        """Return True if any resolvable address string is a private IP."""
        for addr in addrs:
            try:
                if ipaddress.ip_address(addr).is_private:
                    return True
            except ValueError:
                continue
        return False


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
