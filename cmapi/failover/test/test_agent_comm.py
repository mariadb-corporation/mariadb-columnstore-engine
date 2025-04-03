import unittest
import time
import socket
import datetime
import cherrypy
import os.path
from contextlib import contextmanager
from ..agent_comm import AgentComm
from cmapi_server import helpers, node_manipulation
from cmapi_server.failover_agent import FailoverAgent
from mcs_node_control.models.node_config import NodeConfig
from cmapi_server.controllers.dispatcher import dispatcher, jsonify_error
from cmapi_server.managers.certificate import CertificateManager


config_filename = './cmapi_server/cmapi_server.conf'


@contextmanager
def start_server():
    CertificateManager.create_self_signed_certificate_if_not_exist()

    app = cherrypy.tree.mount(root = None, config = config_filename)
    app.config.update({
        '/': {
            'request.dispatch': dispatcher,
            'error_page.default': jsonify_error,
        },
        'config': {
            'path': config_filename,
        },
    })
    cherrypy.config.update(config_filename)

    cherrypy.engine.start()
    cherrypy.engine.wait(cherrypy.engine.states.STARTED)
    yield
    cherrypy.engine.exit()
    cherrypy.engine.block()


class TestAgentComm(unittest.TestCase):

    def test_with_agent_base(self):
        agent = AgentComm()
        # Add events except for enterStandbyMode
        agent.activateNodes(["mysql.com"])
        agent.activateNodes(["mysql.com"])  # an intentional dup
        agent.designatePrimaryNode("mysql.com")
        agent.deactivateNodes(["mysql.com"])
        agent.deactivateNodes(["mysql.com"])
        agent.designatePrimaryNode(socket.gethostname())

        health = agent.getNodeHealth()
        agent.raiseAlarm("Hello world!")
        print("Waiting up to 20s for queued events to be processed and removed")
        stop_time = datetime.datetime.now() + datetime.timedelta(seconds = 20)
        success = False
        while datetime.datetime.now() < stop_time and not success:
            sizes = agent.getQueueSize()
            if sizes != (0, 0):
                time.sleep(1)
            else:
                print("Event queue & deduper are now empty")
                success = True

        print("Waiting for the agent comm thread to die.")
        agent.die()
        self.assertTrue(success)


    # This is the beginnings of an integration test, will need perms to modify the real config file
    def test_with_failover_agent(self):

        print("\n\n")   # make a little whitespace between tests

        # check for existence of and permissions to write to the real config file
        try:
            f = open("/etc/columnstore/Columnstore.xml", "a")
            f.close()
        except PermissionError:
            print(f"Skipping {__name__}, got a permissions error opening /etc/columnstore/Columnstore.xml for writing")
            return

        success = False
        with start_server():
            try:
                agent = FailoverAgent()
                agentcomm = AgentComm(agent)

                # make sure the AC thread has a chance to start before we start issuing cmds.
                # If it grabs jobs in the middle of this block, we'll try to send the config file
                # to mysql.com.  :D
                time.sleep(1)

                # do the same as above.
                agentcomm.activateNodes(["mysql.com"])
                agentcomm.activateNodes(["mysql.com"])  # an intentional dup
                agentcomm.designatePrimaryNode("mysql.com")
                agentcomm.deactivateNodes(["mysql.com"])
                agentcomm.deactivateNodes(["mysql.com"])
                agentcomm.designatePrimaryNode(socket.gethostname())

                health = agent.getNodeHealth()
                agent.raiseAlarm("Hello world!")
                print("Waiting up to 30s for queued events to be processed and removed")
                stop_time = datetime.datetime.now() + datetime.timedelta(seconds = 30)

                while datetime.datetime.now() < stop_time and not success:
                    sizes = agentcomm.getQueueSize()
                    if sizes != (0, 0):
                        time.sleep(1)
                    else:
                        print("Event queue & deduper are now empty")
                        success = True
                if not success:
                    raise Exception("The event queue or de-duper did not empty within 30s")
                agentcomm.die()
            except Exception as e:
                agentcomm.die()
                cherrypy.engine.exit()
                cherrypy.engine.block()
                raise

            # clean up the config file, remove mysql.com
            txnid = helpers.start_transaction()
            node_manipulation.remove_node("mysql.com")
            helpers.update_revision_and_manager()
            helpers.broadcast_new_config()
            helpers.commit_transaction(txnid)
