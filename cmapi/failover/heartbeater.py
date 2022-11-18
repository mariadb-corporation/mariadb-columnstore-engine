import logging
import threading
import time
from socket import socket, SOCK_DGRAM
from struct import pack, unpack_from


class HeartBeater:
    port = 9051
    dieMsg = b'die!00'
    areYouThereMsg = b'AYTM'
    yesIAmMsg = b'YIAM'

    def __init__(self, config, history):
        self.config = config
        self.die = False
        self.history = history
        self.sequenceNum = 0
        self.responseThread = None
        self.sock = None
        self.sockMutex = threading.Lock()
        self.logger = logging.getLogger('heartbeater')

    def start(self):
        self.initSockets()
        self.die = False
        self.responseThread = threading.Thread(
            target=self.listenAndRespond, name='HeartBeater'
        )
        self.responseThread.start()

    def stop(self):
        self.die = True
        # break out of the recv loop
        sock = socket(type=SOCK_DGRAM)
        sock.sendto(self.dieMsg, ('localhost', self.port))
        time.sleep(1)
        self.sock.close()
        self.responseThread.join()

    def initSockets(self):
        self.sock = socket(type=SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', self.port))

    def listenAndRespond(self):
        self.logger.info('Starting the heartbeat listener.')
        while not self.die:
            try:
                self._listenAndRespond()
            except Exception:
                self.logger.warning(
                    'Caught an exception while listening and responding.',
                    exc_info=True
                )
                time.sleep(1)
        self.logger.info('Heartbeat listener exiting normally...')

    def _listenAndRespond(self):
        (data, remote) =  self.sock.recvfrom(300)
        if len(data) < 6:
            return
        (msg_type, seq) = unpack_from('4sH', data, 0)
        if msg_type == self.areYouThereMsg:
            self.logger.trace(f'Got "are you there?" from {remote[0]}')
            name = self.config.who_am_I()
            if name is None:
                self.logger.warning(
                    'Heartbeater: got an "are you there?" msg from '
                    f'{remote[0]}, but this node is not in the list of '
                    'desired nodes for the cluster. '
                    'This node needs a config update.'
                )
                return
            bname = name.encode('ascii')
            if len(bname) > 255:
                bname = bname[:255]
            msg = pack(f'4sH{len(bname)}s', self.yesIAmMsg, seq, bname)
            self.send(msg, remote[0])
            self.logger.trace(f'Send "yes I Am" to {remote[0]}')
        elif msg_type == self.yesIAmMsg:
            if len(data) > 6:
                name = data[6:].decode('ascii')
                self.logger.trace(f'Got "yes I am" from {name}')
                self.history.gotHeartbeat(name, seq)

    def send(self, msg, destaddr):
        self.sockMutex.acquire()
        try:
            self.sock.sendto(msg, (destaddr, self.port))
        except Exception:
            self.logger.warning(
                f'Heartbeater.send(): caught error sending msg to {destaddr}',
                exc_info=True
            )
        finally:
            self.sockMutex.release()

    def sendHeartbeats(self):
        nodes = self.config.getDesiredNodes()
        my_name = self.config.who_am_I()
        msg = pack('4sH', self.areYouThereMsg, self.sequenceNum)
        self.sockMutex.acquire()
        for node in nodes:
            if node == my_name:
                continue
            try:
                self.logger.trace(f'Send "are you there" node {node}')
                self.sock.sendto(msg, (node, self.port))
            except Exception as e:
                pass
                # Suppressing these logs.
                # In docker the whole dns entry gets removed when a container
                # goes away.
                # Ends up spamming the logs until the node is removed from
                # the cluster via the rest endpoint, or the node comes back up.
                # self.logger.warning("Heartbeater.sendHeartbeats():
                # caught an exception sending heartbeat to {}: {}".
                # format(node, e))
        self.sockMutex.release()
        self.sequenceNum = (self.sequenceNum + 1) % 65535
        self.history.setCurrentTick(self.sequenceNum)
