import logging
import socket


MAGIC_BYTES = 0x14fbc137.to_bytes(4, 'little')
# value is tuple(command_bytes, command_value_length)
DBRM_COMMAND_BYTES = {
    'readonly':           ((20).to_bytes(1, 'little'), 0),
    'set_readonly':       ((14).to_bytes(1, 'little'), 0),
    'set_readwrite':      ((15).to_bytes(1, 'little'), 0),
    'set_system_state':   ((55).to_bytes(1, 'little'), 4),
    'get_system_state':   ((54).to_bytes(1, 'little'), 4),
    'clear_system_state': ((57).to_bytes(1, 'little'), 4),
}
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8616
SOCK_TIMEOUT = 5


class DBRMSocketHandler():
    """Class for stream socket operations.

    Include all logic for detecting bytestream protocol version, reading and
    parsing magic inside, getting command bytes and command value length
    by command name.

    """
    long_strings = None

    def __init__(
        self, family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0,
        fileno=None
    ) -> None:
        self._socket = None
        self._family = family
        self._type = type
        self._proto = proto
        self._fileno = fileno
        self._host = None
        self._port = None
        self._recreate_socket()

    @property
    def _connect_called(self):
        """Is connect method called previously.

        This is the instance state to determine if "connect" method called
        previously. This is not quaranteed that connection still alive.
        :return: connected state
        :rtype: bool
        """
        if self._host and self._port:
            return True
        return False

    def _recreate_socket(self) -> None:
        """Create new internal _socket object.

        Create\recreate new _socket object and connects to it if was already
        connected.
        """
        if self._socket is not None:
            self._socket.close()
        self._socket = socket.socket(
            family=self._family, type=self._type,
            proto=self._proto, fileno=self._fileno
        )
        if self._connect_called:
            self.connect(self._host, self._port)

    def _detect_protocol(self) -> None:
        """Detect dbrm socket bytestream version.

        This method normally will be called only in first instance
        at first "send" method call.
        After that header will be formed and parsed depending on
        "long_strings" class variable value.

        Sends "readonly" message with "old" protocol version (before MCS 6.2.1)
        If timeout error raised, sends message with "new" protocol version
        (after MCS 6.2.1) with extra 4 bytes in header.
        If both attemts are failed raise RuntimeError and return the
        "long_strings" variable to initial state - None.

        :raises RuntimeError: [description]
        """
        success = False
        # check at first old protocol because 5.x.x version got an issue if
        # we try to send new format packages.
        for long_strings in (False, True):
            DBRMSocketHandler.long_strings = long_strings
            self.send('readonly')
            try:
                _ = self.receive()
                success = True
                break
            except (socket.timeout, TimeoutError):
                # wrong packet sended could cause errors on the mcs engine side
                self._recreate_socket()
                continue
        if not success:
            # something went wrong so return to unknown protocol state
            DBRMSocketHandler.long_strings = None
            raise RuntimeError(
                'Can\'t detect DBRM bytestream protocol version.'
            )
        else:
            dbrm_protocol_version = (
                'new' if DBRMSocketHandler.long_strings else 'old'
            )
            logging.info(
                f'Detected "{dbrm_protocol_version}" DBRM bytestream protocol'
            )

    def _make_msg(self, command_name: str, command_value: int) -> bytes:
        """Make bytes msg by command name and value.

        :param command_name: name of a command
        :type command_name: str
        :param command_value: command value
        :type command_value: int or None
        :return: msg to send throught socket
        :rtype: bytes
        """
        command_bytes, command_value_length = DBRM_COMMAND_BYTES[command_name]
        data_length = (
            command_value_length + len(command_bytes)
        ).to_bytes(4, 'little')
        # bytestream protocol before MCS 6.2.1 version
        package_header = MAGIC_BYTES + data_length
        if DBRMSocketHandler.long_strings:
            # bytestream protocol after MCS 6.2.1 version
            long_strings_count = (0).to_bytes(4, 'little')
            package_header += long_strings_count

        msg_bytes = package_header + command_bytes
        if command_value is not None:
            msg_bytes += command_value.to_bytes(
                command_value_length, 'little'
            )
        return msg_bytes

    def _receive_magic(self):
        """Reads the stream up to the uncompressed magic.

        The magic is a constant delimeter that occurs at the begging
        of the stream.
        """
        data: bytes
        recv_data: bytes = b''
        while recv_data != MAGIC_BYTES:
            data = self._socket.recv(1)
            # TODO: advanced error handling
            if data == b'':
                raise RuntimeError(
                    'Socket connection broken while receiving magic'
                )
            recv_data += data
            if not MAGIC_BYTES.startswith(recv_data):
                recv_data = data
                continue

    def _receive(self, length: int):
        """Receive raw data from socket by length.

        :param length: length in bytes to receive
        :type length: int
        :raises RuntimeError: if socket connection is broken while receiving
        :return: received bytes
        :rtype: bytes
        """
        chunks = []
        bytes_recd = 0
        while bytes_recd < length:
            chunk = self._socket.recv(min(length - bytes_recd, 2048))
            if chunk == b'':
                raise RuntimeError(
                    'Socket connection broken while receiving data.'
                )
            chunks.append(chunk)
            bytes_recd += len(chunk)
        return b''.join(chunks)

    def _send(self, msg: bytes):
        """Send msg in bytes through the socket.

        :param msg: string in bytes to send
        :type msg: bytes
        :raises RuntimeError: if connection is broken while sending
        """
        totalsent = 0
        while totalsent < len(msg):
            sent = self._socket.send(msg[totalsent:])
            if sent == 0:
                raise RuntimeError(
                    'DBRM socket connection broken while sending.'
                )
            totalsent = totalsent + sent

    def connect(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        """Connect to socket.

        By default it connects with DBRM master.
        """
        self._host = host
        self._port = port
        self._socket.settimeout(SOCK_TIMEOUT)
        self._socket.connect((host, port))

    def close(self):
        """Closing the socket.

        Set _host and _port instance variables to None to change state to
        not connected. Then close the _socket.
        """
        self._host = None
        self._port = None
        self._socket.close()

    def send(self, command_name: str, command_value: int = None):
        """Top level send by command name and value.

        param command_name: name of a command
        :type command_name: str
        :param command_value: command value, defaults to None
        :type command_value: int, optional
        """
        if DBRMSocketHandler.long_strings is None:
            self._detect_protocol()
        msg_bytes = self._make_msg(command_name, command_value)
        self._send(msg_bytes)

    def receive(self):
        """Top level method to receive data from socket.

        Automatically reads the magic and data length from data header.

        :return: received bytes without header
        :rtype: bytes
        """
        self._receive_magic()
        data_length = int.from_bytes(self._receive(4), 'little')
        if DBRMSocketHandler.long_strings:
            # receive long strings count to meet new bytestream protocol
            # requirements (after MCS 6.2.1 release)
            long_strings_count_bytes = self._receive(4)
        data_bytes = self._receive(data_length)
        return data_bytes
