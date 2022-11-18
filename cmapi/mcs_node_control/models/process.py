import os
import time

import psutil

PROCFS_PATH = '/proc/' # Linux only


def open_binary(fname, **kwargs):
    return open(fname, "rb", **kwargs)


def get_host_uptime():
    """
    Return the system boot time expressed in seconds since the epoch.

    :rtype: int : diff b/w current epoch and boot epoch
    """
    path = f'{PROCFS_PATH}stat'
    boot_time = 0
    with open_binary(path) as f:
        for line in f:
            if line.startswith(b'btime'):
                boot_time = float(line.strip().split()[1])
                return int(time.time() - int(boot_time))
        return 0


class Process():
    """An interface to retrieve data from proc."""
    def get_proc_iterator(self):
        for pid in self.pids():
            yield pid


    def pids(self):
        """Returns a list of PIDs currently running on the system."""
        return [int(x) for x in os.listdir(PROCFS_PATH) if x.isdigit()]


    def name(self, pid: int):
        """Method to retrive name associated with the pid."""
        return self.parse_stat_file(pid)['name']


    def parse_stat_file(self, pid: int):
        """Parse /proc/{pid}/stat file and return a dict with various
        process info.

        Using "man proc" as a reference: where "man proc" refers to
        position N always substract 3 (e.g ppid position 4 in
        'man proc' == position 1 in here).
        """
        ret = {}
        try:
            with open_binary(f"{PROCFS_PATH}{pid}/stat") as f:
                data = f.read()
            # Process name is between parentheses. It can contain spaces and
            # other parentheses. This is taken into account by looking for
            # the first occurrence of "(" and the last occurence of ")".
            rpar = data.rfind(b')')
            name = data[data.find(b'(') + 1:rpar]
            fields = data[rpar + 2:].split()

            ret['name'] = name
            ret['status'] = fields[0]
            ret['ppid'] = fields[1]
            ret['ttynr'] = fields[4]
            ret['utime'] = fields[11]
            ret['stime'] = fields[12]
            ret['children_utime'] = fields[13]
            ret['children_stime'] = fields[14]
            ret['create_time'] = fields[19]
            ret['cpu_num'] = fields[36]
            ret['blkio_ticks'] = fields[39]  # aka 'delayacct_blkio_ticks'
        except (PermissionError, ProcessLookupError, FileNotFoundError):
            ret['name'] = ''
            ret['status'] = ''
            ret['ppid'] = ''
            ret['ttynr'] = ''
            ret['utime'] = ''
            ret['stime'] = ''
            ret['children_utime'] = ''
            ret['children_stime'] = ''
            ret['create_time'] = ''
            ret['cpu_num'] = ''
            ret['blkio_ticks'] = ''  # aka 'delayacct_blkio_ticks'
        return ret

    @staticmethod
    def check_process_alive(proc_name: str) -> bool:
        """Check process running.

        :param proc_name: process name
        :type proc_name: str
        :return: True if process running, otherwise False
        :rtype: bool
        """
        # Iterate over the all the running process
        for proc in psutil.process_iter():
            try:
                # Check if process name equals to the given name string.
                if proc_name.lower() == proc.name().lower():
                    return True
            except (
                psutil.NoSuchProcess, psutil.AccessDenied,
                psutil.ZombieProcess
            ):
                pass
        return False
