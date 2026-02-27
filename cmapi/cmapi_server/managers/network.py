import errno
import fcntl
import logging
import socket
import struct
from dataclasses import dataclass
from ipaddress import ip_address
from typing import List, Optional, cast

from cmapi_server.exceptions import CMAPIBasicError

try:
    import psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None
    _PSUTIL_AVAILABLE = False



SIOCGIFADDR = 0x8915  # SIOCGIFADDR "socket ioctl get interface address"


class NetworkManager:
    @classmethod
    def get_ip_version(cls, ip_addr: str) -> int:
        """Get version of a given IP address.

        :param ip_addr: IP to get version
        :type ip_addr: str
        :return: version of a given IP
        :rtype: int
        """
        return ip_address(ip_addr).version

    @classmethod
    def is_ip(cls, input_str: str) -> bool:
        """Check is input a valid IP or not.

        :param input_str: input string
        :type input_str: str
        :return: True if input is a valid IP
        :rtype: bool
        """
        try:
            ip_address(input_str)
            return True
        except ValueError:
            return False

    @classmethod
    def resolve_hostname_to_ips(
        cls,
        hostname: str,
        only_ipv4: bool = True,
        exclude_loopback: bool = False
    ) -> list[str]:
        """Resolve a hostname to one or more IP addresses.

        :param hostname: Hostname to resolve.
        :type hostname: str
        :param only_ipv4: Return only IPv4 addresses (default: True).
        :type only_ipv4: bool
        :param exclude_loopback: Exclude loopback addresses like 127.x.x.x (default: True).
        :type exclude_loopback: bool
        :return: List of resolved IP addresses.
        :rtype: list[str]
        """
        sorted_ips: list[str] = []
        try:
            addr_info = socket.getaddrinfo(
                hostname,
                None,
                socket.AF_INET if only_ipv4 else socket.AF_UNSPEC,
                socket.SOCK_STREAM
            )
            ip_set = {
                info[4][0] for info in addr_info
                if not (exclude_loopback and ip_address(info[4][0]).is_loopback)
            }
            sorted_ips = sorted(
                list(ip_set),
                key=lambda ip: (
                    not ip_address(ip).is_loopback,  # loopback first (False < True)
                    ip_address(ip).version != 4,     # IPv4 before IPv6 (False < True)
                    ip_address(ip)                   # lexical order
                )
            )
        except socket.gaierror:
            logging.error(
                f'Standard name resolution failed for hostname: {hostname!r}',
                exc_info=True
            )

        return sorted_ips

    @classmethod
    def get_ip_address_by_nic(cls, ifname: str) -> str:
        """Get IP address of a network interface.

        :param ifname: network interface name
        :type ifname: str
        :return: ip address
        :rtype: str
        """
        # doesn't work on Windows,
        # OpenBSD and probably doesn't on FreeBSD/pfSense either
        ip_addr: str = ''
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                ip_addr = socket.inet_ntoa(
                    fcntl.ioctl(
                        s.fileno(),
                        SIOCGIFADDR,
                        struct.pack('256s', bytes(ifname[:15], 'utf-8'))
                    )[20:24]
                )
        except OSError as exc:
            if exc.errno == errno.ENODEV:
                logging.error(
                    f'Interface {ifname!r} doesn\'t exist.'
                )
            else:
                logging.error(
                    f'Unknown OSError code while getting IP for an {ifname!r}',
                    exc_info=True
                )
        except Exception:
            logging.error(
                (
                    'Unknown exception while getting IP address of an '
                    f'{ifname!r} interface',
                ),
                exc_info=True
            )
        return ip_addr

    @classmethod
    def get_current_node_ips(
        cls, ignore_loopback: bool = False, only_ipv4: bool = True,
    ) -> list[str]:
        """Get all IP addresses for all existing network interfaces.


        :param ignore_loopback: Ignore loopback addresses, defaults to False
        :type ignore_loopback: bool, optional
        :param only_ipv4: Return only IPv4 addresses, defaults to True
        :type only_ipv4: bool, optional
        :return: IP addresses for all node interfaces
        :rtype: list[str]
        :raises CMAPIBasicError: If no IPs are found
        """
        ext_ips: list[str] = []
        loopback_ips: list[str] = []

        if _PSUTIL_AVAILABLE:
            try:
                for _, addrs in psutil.net_if_addrs().items():
                    for addr in addrs:
                        if only_ipv4 and addr.family != socket.AF_INET:
                            continue
                        try:
                            ip = ip_address(addr.address)
                            if ip.is_loopback:
                                loopback_ips.append(str(ip))
                            else:
                                ext_ips.append(str(ip))
                        except ValueError:
                            continue  # Not a valid IP (e.g., MAC addresses)
            except Exception:
                logging.warning(
                    'Failed to get IPs via psutil, falling back to ioctl',
                    exc_info=True
                )

            result = ext_ips if ignore_loopback else [*ext_ips, *loopback_ips]
            if result:
                return result
            logging.warning(
                'psutil returned no valid IPs, trying fallback method'
            )

        ext_ips: list[str] = []
        loopback_ips: list[str] = []
        # Fallback to stdlib method using fcntl/ioctl
        for _, nic_name in socket.if_nameindex():
            ip_addr = cls.get_ip_address_by_nic(nic_name)
            if not ip_addr:
                continue
            if only_ipv4 and cls.get_ip_version(ip_addr) != 4:
                continue
            if ip_address(ip_addr).is_loopback:
                loopback_ips.append(ip_addr)
            else:
                ext_ips.append(ip_addr)

        result = ext_ips if ignore_loopback else [*ext_ips, *loopback_ips]
        if not result:
            raise CMAPIBasicError('No IP addresses found on this node.')
        return result

    @classmethod
    def get_hostname(cls, ip_addr: str) -> Optional[str]:
        """Get hostname for a given IP address.

        :param ip_addr: IP address to get hostname
        :type ip_addr: str
        :return: Hostname if it exists, otherwise None
        :rtype: Optional[str]
        """
        try:
            hostnames = socket.gethostbyaddr(ip_addr)
            return hostnames[0]
        except socket.herror:
            logging.error('No hostname found for address: %s', ip_addr)
            return None

    @classmethod
    def get_hostnames_by_ip(cls, ip_addr: str) -> list[str]:
        """Get all hostnames for a given IP address.

        :return: List of hostnames (may be empty if reverse lookup fails)
        """
        try:
            primary, aliases, _ = socket.gethostbyaddr(ip_addr)
            seen = set()
            names = []
            for n in [primary, *aliases]:
                if n not in seen:
                    seen.add(n)
                    names.append(n)
            return names
        except socket.herror:
            logging.error('No hostname found for address: %s', ip_addr)
            return []

    @classmethod
    def is_only_loopback_hostname(cls, hostname: str) -> bool:
        """Check if all IPs resolved from the hostname are loopback.

        :param hostname: Hostname to check
        :type hostname: str
        :return: True if all resolved IPs are loopback, False otherwise
        :rtype: bool
        """
        ips = cls.resolve_hostname_to_ips(hostname)
        if not ips:
            return False
        for ip in ips:
            if not ip_address(ip).is_loopback:
                return False
        return True

    @classmethod
    def resolve_ip_and_hostname(cls, input_str: str) -> tuple[str, Optional[str]]:
        """Resolve input string to an (IP, hostname) pair.

        :param input_str: Input which may be an IP address or a hostname
        :type input_str: str
        :return: A tuple containing (ip, hostname)
        :rtype: tuple[str, str]
        :raises CMAPIBasicError: if hostname resolution yields no IPs
        """
        ip: str = ''
        hostname: Optional[str] = None

        if cls.is_ip(input_str):
            ip = input_str
            hostname = cls.get_hostname(input_str)
        else:
            hostname = input_str
            ip_list = cls.resolve_hostname_to_ips(
                input_str,
                exclude_loopback=not cls.is_only_loopback_hostname(input_str)
            )
            if not ip_list:
                raise CMAPIBasicError(f'No IPs found for {hostname!r}')
            ip = ip_list[0]
        return ip, hostname

    @classmethod
    def validate_hostname_fwd_rev(cls, hostname: str) -> None:
        """Validate forward and reverse DNS for a hostname.

        Checks that hostname resolves to one or more usable IPs and that at
        least one of those IPs reverse-resolves back to the provided hostname
        (either an exact match or an FQDN starting with the hostname are accepted).

        :raises CMAPIBasicError: if validation fails
        """
        exclude_loopback = not cls.is_only_loopback_hostname(hostname)
        ips = cls.resolve_hostname_to_ips(
            hostname,
            only_ipv4=True,
            exclude_loopback=exclude_loopback,
        )

        if not ips:
            raise CMAPIBasicError(
                f"Hostname {hostname!r} did not resolve to any usable IPs. "
                "Please fix DNS or add the host by IP."
            )

        wanted = hostname.rstrip('.').lower()
        for ip in ips:
            rev_names = cls.get_hostnames_by_ip(ip)
            for rev in rev_names:
                rev_norm = rev.rstrip('.').lower()
                # Accept exact match ("db1" == "db1") or FQDN starting with the short hostname
                # e.g. user provided "db1" and PTR returns "db1.example.com"
                if rev_norm == wanted or rev_norm.startswith(wanted + '.'):
                    return

        raise CMAPIBasicError(
            'Forward/reverse DNS check failed: '
            f"hostname {hostname!r} resolved to {ips}, but none of these IPs "
            f"reverse-resolve back to {hostname!r}. Consider adding the host by IP, "
            'or fix DNS so that at least one IP has a PTR/record mapping back to '
            'the provided hostname.'
        )
