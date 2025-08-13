import concurrent.futures
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

from fabric import Connection


from dataclasses import dataclass, field

@dataclass
class RemoteHost:
    """Keeps configuration for a host in the cluster"""
    name: str
    private_ip: str
    public_fqdn: str
    key_file_path: Path = field(repr=False)
    ssh_user: str = field(repr=False)

    @contextmanager
    def ssh_connection(self):
        """Connect to the host using SSH (via Fabric)"""
        conn = Connection(
            host=self.public_fqdn,
            user=self.ssh_user,
            connect_kwargs={
                "key_filename": str(self.key_file_path),
            }
        )

        yield conn

        conn.close()

    def is_reachable(self) -> bool:
        """Check if the host is reachable via SSH"""
        try:
            with self.ssh_connection() as conn:
                conn.run("echo 'SSH connection successful'", hide=True)
            return True
        except Exception as e:
            print(f"SSH connection failed for {self.name}: {e}")
            return False

    def exec_mcs(self, params: str, drop_timestamp: bool = True) -> dict:
        """Execute an mcs subcommand and return its parsed output"""
        with self.ssh_connection() as conn:
            print(f"{self.name}: Executing 'mcs {params}'")
            result = conn.sudo(f"mcs {params}", hide=True)
            if result.return_code != 0:
                raise RuntimeError(f"Failed to execute mcs command on {self.name}: {result.stderr}")
            output = json.loads(result.stdout)
            return output


class ClusterConfig:
    """Keeps configuration of the cluster"""
    def __init__(self, mcs_hosts: list[RemoteHost], maxscale_hosts: list[RemoteHost]) -> None:
        self.mcs_hosts = mcs_hosts
        self.maxscale_hosts = maxscale_hosts

    def __repr__(self) -> str:
        return f"ClusterConfig(mcs_hosts={self.mcs_hosts}"

    @property
    def primary(self) -> RemoteHost:
        return self.mcs_hosts[0]

    @property
    def replicas(self) -> list[RemoteHost]:
        return self.mcs_hosts[1:]


def run_on_all_hosts_parallel(
      hosts: list[RemoteHost],
      func: Callable[[RemoteHost], Any],
      timeout: float | None = None,
      max_workers: int | None = None,
    ) -> dict[str, Any]:
    """
    Run a function on all hosts in parallel.

    :param hosts: List of RemoteHost objects
    :param func: Function that takes a RemoteHost as its only argument
    :param timeout: Timeout for the function execution
    :param max_workers: Maximum number of worker threads (None = default based on system)
    :returns: Dictionary mapping hosts to function results
    """
    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start the function execution on each host
        future_to_host = {executor.submit(func, host): host for host in hosts}

        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_host):
            host = future_to_host[future]
            try:
                result = future.result(timeout=timeout)
                results[host.name] = result
            except Exception as exc:
                print(f"Error on host {host.name}: {exc}")
                results[host.name] = exc
                raise

    return results


def block_port(host: RemoteHost, port: int) -> None:
    """Block a port on the host using iptables"""
    with host.ssh_connection() as conn:
        print(f"{host.name}: Blocking port {port}")
        conn.sudo(f"iptables -A INPUT -p tcp --dport {port} -j DROP")
        print(f"{host.name}: Port {port} blocked")


def unblock_port(host: RemoteHost, port: int) -> None:
    """Unblock a port on the host using iptables"""
    with host.ssh_connection() as conn:
        print(f"{host.name}: Unblocking port {port}")
        conn.sudo(f"iptables -D INPUT -p tcp --dport {port} -j DROP")
        print(f"{host.name}: Port {port} unblocked")