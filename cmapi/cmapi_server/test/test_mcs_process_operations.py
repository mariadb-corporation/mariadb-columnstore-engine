import os

from cmapi_server.managers.process import MCSProcessManager
from cmapi_server.process_dispatchers.systemd import SystemdDispatcher
from cmapi_server.test.unittest_global import (
    DDL_SERVICE, CONTROLLERNODE_SERVICE, SYSTEMCTL,
    BaseProcessDispatcherCase
)

class SystemdTest(BaseProcessDispatcherCase):

    def test_systemd_status_start(self):
        os.system(f'{SYSTEMCTL} stop {DDL_SERVICE}')
        self.assertFalse(
            SystemdDispatcher.is_service_running(service=DDL_SERVICE, use_sudo=False)
        )
        self.assertTrue(SystemdDispatcher.start(service=DDL_SERVICE, use_sudo=False))

        os.system(f'{SYSTEMCTL} stop {CONTROLLERNODE_SERVICE}')
        self.assertFalse(
            SystemdDispatcher.is_service_running(service=CONTROLLERNODE_SERVICE, use_sudo=False)
        )
        result = SystemdDispatcher.start(service=CONTROLLERNODE_SERVICE, use_sudo=False)
        self.assertTrue(result)
        self.assertTrue(
            SystemdDispatcher.is_service_running(service=CONTROLLERNODE_SERVICE, use_sudo=False)
        )

    def test_systemd_status_stop(self):
        os.system(f'{SYSTEMCTL} start {CONTROLLERNODE_SERVICE}')
        self.assertTrue(
            SystemdDispatcher.is_service_running(service=CONTROLLERNODE_SERVICE, use_sudo=False)
        )
        self.assertTrue(SystemdDispatcher.stop(service=CONTROLLERNODE_SERVICE, use_sudo=False))
        self.assertFalse(
            SystemdDispatcher.is_service_running(service=CONTROLLERNODE_SERVICE, use_sudo=False)
        )

    def test_systemd_status_restart(self):
        os.system(f'{SYSTEMCTL} start {CONTROLLERNODE_SERVICE}')
        self.assertTrue(
            SystemdDispatcher.is_service_running(service=CONTROLLERNODE_SERVICE, use_sudo=False)
        )
        self.assertTrue(SystemdDispatcher.restart(service=CONTROLLERNODE_SERVICE, use_sudo=False))
        self.assertTrue(
            SystemdDispatcher.is_service_running(service=CONTROLLERNODE_SERVICE, use_sudo=False)
        )

        os.system(f'{SYSTEMCTL} stop {CONTROLLERNODE_SERVICE}')
        self.assertFalse(
            SystemdDispatcher.is_service_running(service=CONTROLLERNODE_SERVICE, use_sudo=False)
        )
        self.assertTrue(SystemdDispatcher.restart(service=CONTROLLERNODE_SERVICE, use_sudo=False))
        self.assertTrue(
            SystemdDispatcher.is_service_running(service=CONTROLLERNODE_SERVICE, use_sudo=False)
        )


class MCSProcessManagerTest(BaseProcessDispatcherCase):

    def get_systemd_serv_name(self, service_name):
        if service_name == 'mcs-workernode':
            return f'{service_name}@1'
        return service_name

    def test_mcs_process_manager(self):
        MCSProcessManager.detect('systemd', '')
        for prog in MCSProcessManager._get_sorted_progs(is_primary=True, reverse=True).values():
            serv_name = self.get_systemd_serv_name(prog.service_name)
            os.system(f'{SYSTEMCTL} stop {serv_name}')
        self.assertIsNone(MCSProcessManager.start_node(is_primary=True, use_sudo=False))

        for prog in MCSProcessManager.mcs_progs.values():
            serv_name = self.get_systemd_serv_name(prog.service_name)
            if serv_name == 'mcs-storagemanager':
                continue
            self.assertTrue(
                MCSProcessManager.process_dispatcher.is_service_running(
                    service=serv_name, use_sudo=False
                )
            )

        self.assertIsNone(MCSProcessManager.stop_node(is_primary=True, use_sudo=False))
        for prog in MCSProcessManager.mcs_progs.values():
            serv_name = self.get_systemd_serv_name(prog.service_name)
            self.assertFalse(
                MCSProcessManager.process_dispatcher.is_service_running(
                    service=serv_name, use_sudo=False
                )
            )
        self.assertEqual(len(MCSProcessManager.get_running_mcs_procs()), 0)
        self.assertTrue(
            MCSProcessManager.is_node_processes_ok(
                is_primary=True, node_stopped=True
            )
        )

        for prog in MCSProcessManager._get_sorted_progs(is_primary=True).values():
            serv_name = self.get_systemd_serv_name(prog.service_name)
            os.system(f'{SYSTEMCTL} start {serv_name}')

        for prog in MCSProcessManager.mcs_progs.values():
            serv_name = self.get_systemd_serv_name(prog.service_name)
            self.assertTrue(
                MCSProcessManager.process_dispatcher.is_service_running(
                    service=serv_name, use_sudo=False
                )
            )
        self.assertEqual(
            len(MCSProcessManager.get_running_mcs_procs()),
            len(MCSProcessManager.mcs_progs.keys())
        )
        self.assertTrue(
            MCSProcessManager.is_node_processes_ok(
                is_primary=True, node_stopped=False
            )
        )
