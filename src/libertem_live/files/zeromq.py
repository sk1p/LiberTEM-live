#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import ast
import numpy

try:
    import Queue as queue
except ImportError:
    import queue

import socket
import sys
import threading
import time
import zmq


_serialisation_schemes = []
if sys.version_info < (3, ):
    PYTHON_VERSION = 2
else:
    PYTHON_VERSION = 3

try:
    import cPickle as pickle
    _serialisation_schemes.append('pickle')
except ImportError:
    import pickle
    _serialisation_schemes.append('pickle')

try:
    import msgpack
    _serialisation_schemes.append('msgpack')
except ImportError:
    pass

try:
    import json
    _serialisation_schemes.append("json")
except ImportError:
    pass

# fixme
# + add possibility for different protocols
# + add general communication class to join sending and receiving.
# + add support for different serialisation schemes: msgpack, json
# + add method to send data as a binary blob
# + add auto port selection possibility


# auto get port example
# context = zmq.Context()
# connection = context.socket(zmq.REP)
# connection.bind('tcp://*:*')
# connection.getsockopt(zmq.LAST_ENDPOINT)
# reply is: 'tcp://0.0.0.0:57983'
# connection.close()
# context.term()

__all__ = [
    'ClientPub',
    'ClientPull',
    'ClientPush',
    'ClientRep',
    'ClientReq',
    'ClientSub',
    'ServerPub',
    'ServerPull',
    'ServerPush',
    'ServerRep',
    'ServerReq',
    'ServerSub',
    'ZmqPoller',
    'ZmqPullRepQueue'
]


###########
# _ZeroMQ #
###########

class _ZeroMQ(object):
    '''
    Base zeromq class.
    '''

    def __init__(
            self,
            linger=None,
            serialisation=None,
            proxy_connection=False,
            verbosity=0):
        '''
        Class of which others should inherit.

        Parameters
        ----------
        linger : float, optional
            Linger time in s before closing the socket.

        serialisation : str, optional
            The serialisation scheme to use.

        proxy_connection : boolean
            Enable when connecting a socket to a proxy socket.

        verbosity : int, optional
            The verbosity level.
            Default is 0.

        Info
        ----
        When using msgpack as serialisation there is no possible way to keep
         the tuple dtype, it is converted to a list and will stay as a list
         at the receiving end.
        '''

        self.serialisation = serialisation
        self.verbosity = verbosity
        self._proxy_connection = proxy_connection

        # proxy related variables.
        self._sender_address = None
        self._BE_READY = 'backend_ready_announcement'

        self._defaults = {}
        self._defaults['linger'] = 1
        self._subscription_list = []

        if 'pickle' in _serialisation_schemes:
            self._defaults['serialisation'] = 'pickle'
        elif 'msgpack' in _serialisation_schemes:
            self._defaults['serialisation'] = 'msgpack'
        else:
            self._defaults['serialisation'] = None

        if self.verbosity > 1:
            print('Supported serialisation schemes: {}'.format(
                _serialisation_schemes
                )
            )

        self._set_linger_time(linger)

    ###########
    # __del__ #
    ###########

    def __del__(self):
        self.stop()

    #############
    # __enter__ #
    #############

    def __enter__(self):
        return self

    ############
    # __exit__ #
    ############

    def __exit__(self, *args):
        if self.verbosity > 5:
            print('Exit called.')
        self.stop()

    ###########################
    # _add_subscription_topic #
    ###########################

    def _add_subscription_topic(self, topic):
        '''
        Adds a subscription topic to a socket.

        Parameters
        ----------
        topic : str
            The topic to subscribe to.
        '''

        if PYTHON_VERSION == 2:
            if isinstance(topic, str):
                topic = unicode(topic)  # noqa F821

        if (len(self._subscription_list) == 1
                and self._subscription_list[0] == ''):
            self._remove_subscription_topic('')

        if topic not in self._subscription_list:
            self._subscription_list.append(topic)
            self._set_socket_option(zmq.SUBSCRIBE, topic)

    #########
    # _bind #
    #########

    def _bind(self, host, port, protocol, connection_type, hwm=None):
        """
        Binds to a socket.

        Parameters
        ----------
        host : str
            The host address.

        port : int
            The port.

        protocol : str
            The bind protocol.

        connection_type : str
            The connection type.

        hwm : int
            The high water mark.
        """
        self._create_context()
        self._create_socket(connection_type)
        bind_address = self._get_bind_address(
            host,
            port,
            protocol=protocol
        )

        if self.verbosity > 3:
            print('Binding to: {}'.format(
                bind_address
                )
            )

        if hwm is not None:
            self._set_high_water_mark(hwm)

        self._connection.bind(bind_address)

        # Sleep to allow for the asynchronous bind.
        time.sleep(0.2)

        protocol, host, port = self.get_connection_details()

        if self.verbosity > 0:
            print('Bound to: {}://{}:{}'.format(
                protocol,
                host,
                port
                )
            )

        self._connection_mode = 'server'

    #######################
    # _bytes_decode_check #
    #######################

    def _bytes_decode_check(self, str_variable, codec="utf-8", evaluate=False):
        """
        Checks if the variable needs to be converted into a string array.

        Parameters
        ----------
        str_variable : any
            The variable to check.

        codec : str, optional
            The coded used for decoding.
            Default is UTF-8.

        evaluate : boolean, optional
            Does a literal evaluation. Only needed for python 2.

        Returns
        -------
        str
            Memoryview compatible string type.
        """

        # Needed to recover dicts, ...
        if PYTHON_VERSION == 2 and evaluate:
            try:
                str_variable = ast.literal_eval(str_variable)
            except Exception:
                pass

        if isinstance(str_variable, bytes):
            str_variable = str_variable.decode(codec)

        return str_variable

    #######################
    # _bytes_encode_check #
    #######################

    def _bytes_encode_check(self, str_variable, codec="utf-8"):
        """
        Checks if the variable needs to be converted into a bytes array.

        Parameters
        ----------
        str_variable : str
            The string variable to check.

        codec : str, optional
            The codec used for encoding,
            Default is UTF-8.

        Returns
        -------
        str
            Memoryview compatible string type.
        """

        # For python3
        if PYTHON_VERSION > 2:
            if isinstance(str_variable, str):
                str_variable = str_variable.encode("utf-8")
        else:
            if isinstance(str_variable, unicode):   # noqa F821
                str_variable = str_variable.encode("utf-8")
            else:
                str_variable = str(str_variable)

        return str_variable

    ############
    # _connect #
    ############

    def _connect(self, host, port, protocol, connection_type, hwm=None):
        '''
        Connects to a host:port.

        Parameters
        ----------
        host : str
            The host name.

        port : int
        '''
        self._create_context()
        self._create_socket(connection_type)
        bind_address = self._get_bind_address(
            host,
            port,
            protocol=protocol
        )
        self._connection.connect(bind_address)

        # Sleep to allow for the asynchronous bind.
        time.sleep(0.2)

        if self.verbosity > 0:
            _, host, port = self.get_connection_details()
            print('Connection opened to {}:{}'.format(
                host,
                port
                )
            )

        self._connection_mode = 'client'

    ###################
    # _create_context #
    ###################

    def _create_context(self, nb_io_threads=1):
        '''
        Creates a ZMQ context.

        Parameters
        ----------
        nb_io_threads : int, optional
            The number of I/O threads to use.
            Default is 1.
        '''

        self._context = zmq.Context()
        self._context.setsockopt(zmq.IO_THREADS, nb_io_threads)

    ##################
    # _create_socket #
    ##################

    def _create_socket(self, socket_type):
        '''
        Creates the ZMQ socket.

        Parameters
        ----------
        socket_type : object
            The ZMQ socket type object
        '''

        self._connection = self._context.socket(socket_type)

    ################
    # _deserialise #
    ################

    def _deserialise(self, data, serialisation):
        '''
        Deserialises the data.

        Parameters
        ----------
        data : any
            The serialised data.

        serialisation : str
            The serialisation type.

        Returns
        -------
        any
            The deserialised data.
        '''

        if serialisation not in _serialisation_schemes:
            raise ValueError('Serialisation scheme not supported.')

        if serialisation == 'json':
            if isinstance(data, numpy.dtype):
                data = numpy.dtype(data)

            msg = json.loads(data)

        elif serialisation == 'msgpack':
            if isinstance(data, numpy.dtype):
                data = numpy.dtype(data)

            # raw option is only needed for msgpack < 1.0.0
            # version > have this as a default option.
            msg = msgpack.unpackb(
                data,
                raw=False,
                strict_map_key=False
            )

        elif serialisation == 'pickle':
            if type(data) == numpy.ndarray:
                msg = data
            else:
                msg = pickle.loads(data)

        return msg

    ############################
    # _deserialise_numpy_array #
    ############################

    def _deserialise_numpy_array(
            self,
            serialised_array,
            serialised_shape,
            serialised_dtype,
            serialisation,
            serialised_metadata=None):
        '''
        Deserialises a numpy array.

        Parameters
        ----------
        serialised_array : serialised object
            The serialised array.

        serialised_shape : serialised object
            The serialised shape of the array.

        serialised_dtype : serialised object
            The serialised dtype of the array.

        serialisation : str
            The serialisation scheme to use.

        serialised_metadata : serialised object, optional
            Serialised dictionary containing metadata.

        Returns
        -------

        '''
        # _array = self._deserialise(
        #     serialised_array,
        #     serialisation
        # )

        shape = self._deserialise(
            serialised_shape,
            serialisation
        )

        dtype = self._deserialise(
            serialised_dtype,
            serialisation
        )

        if serialised_metadata is not None:
            metadata = self._deserialise(
                serialised_metadata,
                serialisation
            )
        else:
            metadata = None

        if (
                serialisation == 'json'
                or serialisation == 'msgpack'):
            dtype = numpy.dtype(dtype)

        array = numpy.frombuffer(
            # buffer(_array),
            # _array,
            serialised_array,
            dtype=dtype
        )

        # print(array)

        array = array.reshape(shape)

        return array, metadata

    ####################
    # get_bind_address #
    ####################

    def _get_bind_address(self, host, port=None, protocol=None):
        '''
        Creates the binding address.

        Parameters
        ----------
        host : str
            Host name of the server.

        port : int or str
            Port number to bind or connect to.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        Returns
        -------
        str
            Binding address based on IP address.
        '''

        # Port range check.
        if port is not None:
            if 0 < port < 1024 or port > 2**16 - 1:
                raise ValueError('Provided port not in range.')
        else:
            port = '*'

        # Default protocol
        if protocol is None:
            protocol = 'tcp'

        if protocol not in ['inproc', 'ipc', 'tcp', 'pgm', 'epgm']:
            raise ValueError(
                'Unsupported socket protocol.'
            )

        # Localhost conversion.
        if (host == 'localhost'
                or host == '127.0.0.1'):
            host = socket.getfqdn(socket.gethostname())

        # Convert hostname to IP.
        if host != '*':
            host_address = socket.gethostbyname(host)
        else:
            host_address = host

        if self.verbosity > 0:
            print(
                'host: {}\n'
                'address: {}\n'
                'port: {}'.format(
                    host,
                    host_address,
                    port
                )
            )

        # Auto port host correction
        if port == '*':
            if host_address == socket.gethostbyname(socket.gethostname()):
                host_address = '*'
            else:
                raise ValueError(
                    'Auto port selection only works for binding '
                    'to the local host.'
                )

        bind_address = (
            protocol
            + '://'
            + host_address
            + ':'
            + str(port)
        )

        return bind_address

    #######################
    # _get_sender_address #
    #######################

    def _get_sender_address(self):
        '''
        Returns the message sender address.

        Returns
        -------
        str
            The message sender address.
        '''

        return self._sender_address

    #################
    # _poller_check #
    #################

    def _poller_check(self, poller, timeout):
        '''
        Checks the poller on the socket for new messages.

        Parameters
        ----------
        poller : instance
            The poller instance.

        timeout : float
            Timeout in seconds.

        Returns
        -------
        boolean
            True when a message is available, False when not.
        '''

        return poller.check(timeout)

    ################
    # _poller_init #
    ################

    def _poller_init(self, mode):
        '''
        Inits the poller on the socket.

        Parameters
        ----------
        mode : str
            The mode of the poller.

        Returns
        -------
        object
            Object to the poller.
        '''

        return ZmqPoller(self._connection, mode)

    ####################
    # _receive_message #
    ####################

    def _receive_message(self):

        # Connected to a proxy connection.
        if self._proxy_connection:
            (
                self._sender_address,
                _,
                topic,
                serialised_msg,
                serialisation,
                metadata) = self._connection.recv_multipart()

        # Not connected to a proxy.
        else:
            (
                topic,
                serialised_msg,
                serialisation,
                metadata) = self._connection.recv_multipart()

        serialisation = self._bytes_decode_check(serialisation)
        topic = self._bytes_decode_check(topic)

        if serialisation is not None:
            msg = self._deserialise(serialised_msg, serialisation)
        else:
            msg = serialised_msg

        msg = self._bytes_decode_check(msg, evaluate=True)

        return topic, msg, metadata

    ##################
    # _receive_multi #
    ##################

    def _receive_multi(self):
        '''
        Receives a multi part data message.
        '''

        # Connected to a proxy connection.
        if self._proxy_connection:
            rcv_msg = self._connection.recv_multipart()
            (
                self._sender_address,
                _,
                topic,
                serialisation,
                serialised_metadata) = rcv_msg[0:5]

            payload = rcv_msg[5::]

        # Not connected to a proxy.
        else:
            rcv_msg = self._connection.recv_multipart()

            (
                topic,
                serialisation,
                serialised_metadata) = rcv_msg[0:3]

            payload = rcv_msg[3::]

        serialisation = self._bytes_decode_check(serialisation)
        topic = self._bytes_decode_check(topic)

        metadata = self._deserialise(
            serialised_metadata,
            serialisation
        )
        data = []
        for dataset_nb in range(len(metadata['_dataset_type'])):
            if metadata['_dataset_type'][dataset_nb] == 'ndarray':
                array, _ = self._deserialise_numpy_array(
                    payload.pop(0),
                    payload.pop(0),
                    payload.pop(0),
                    serialisation
                )

                data.append(array)

            else:
                msg = self._deserialise(
                    payload.pop(0),
                    serialisation
                )

                data.append(msg)

        metadata.pop('_dataset_type')

        return topic, data, metadata

    ########################
    # _receive_numpy_array #
    ########################

    def _receive_numpy_array(self):
        '''
        Receive a numpy array.

        Returns
        -------
        str
            The topic of the message.

        ndarray
            The received array.

        metadata
            The metadata that was attached.
        '''

        # Connected to a proxy connection.
        if self._proxy_connection:
            (self._sender_address,
                _,
                topic,
                serialised_array,
                serialised_shape,
                serialised_dtype,
                serialisation,
                serialised_metadata) = self._connection.recv_multipart()

        # Not connected to a proxy.
        else:
            (topic,
                serialised_array,
                serialised_shape,
                serialised_dtype,
                serialisation,
                serialised_metadata) = self._connection.recv_multipart()

        serialisation = self._bytes_decode_check(serialisation)

        array, metadata = self._deserialise_numpy_array(
            serialised_array,
            serialised_shape,
            serialised_dtype,
            serialisation,
            serialised_metadata
        )

        topic = self._bytes_decode_check(topic)

        return topic, array, metadata

    ##############################
    # _remove_subscription_topic #
    ##############################

    def _remove_subscription_topic(self, topic):
        '''
        Removes a subscription topic to a socket.

        Parameters
        ----------
        topic : str
            The topic to subscribe to.
        '''

        if topic in self._subscription_list:
            self._set_socket_option(zmq.UNSUBSCRIBE, topic)
            self._subscription_list.remove(topic)

    #################
    # _send_message #
    #################

    def _send_message(
            self,
            message,
            topic=None,
            metadata=None,
            serialisation=None):
        '''
        Send a message.

        Parameters
        ----------
        message : any
            Message to be send.

        topic : str, optional
            Topic of the message, mainly used for pub-sub sockets.

        serialisation : str, optional
            Serialisation scheme. Default is JSON.
        '''
        if topic is None:
            topic = ''

        if metadata is None:
            metadata = {}

        topic = self._bytes_encode_check(topic)
        if serialisation in ["pickle", "msgpack"]:
            message = self._bytes_encode_check(message)

        serialised_msg, serialisation = self._serialise(
            message,
            serialisation=serialisation
        )

        if metadata is not None:
            serialised_metadata, serialisation = self._serialise(
                metadata,
                serialisation=serialisation
            )
        else:
            serialised_metadata = metadata

        if serialisation in ["json"]:
            serialised_msg = self._bytes_encode_check(serialised_msg)
            serialised_metadata = self._bytes_encode_check(serialised_metadata)
        serialisation = self._bytes_encode_check(serialisation)

        # fixme: should check if topic is needed
        # Connected to a proxy connection.
        if self._proxy_connection and self._sender_address is not None:
            self._connection.send_multipart(
                [
                    self._sender_address,
                    b'',
                    topic,
                    serialised_msg,
                    serialisation,
                    serialised_metadata
                ]
            )

        else:
            self._connection.send_multipart(
                [
                    topic,
                    serialised_msg,
                    serialisation,
                    serialised_metadata
                ]
            )

    ###############
    # _send_multi #
    ###############

    def _send_multi(
            self,
            data,
            metadata=None,
            topic=None,
            serialisation=None):
        '''
        Sends multiple datasets.

        Parameters
        ----------
        data : list
            A list containing everything you want to send.

        metadata : dict, optional
            Dictionary containing extra information.

        topic : str, optional
            Topic used for the message.

        serialisation : str, optional
            Serialisation scheme to use.
        '''

        if topic is None:
            topic = ''

        if metadata is None:
            metadata = {}

        topic = self._bytes_encode_check(topic)

        payload = []

        metadata['_dataset_type'] = []

        for datapart in data:
            if isinstance(datapart, numpy.ndarray):
                metadata['_dataset_type'].append('ndarray')
                (serialised_array,
                    serialised_shape,
                    serialised_dtype,
                    _,
                    serialisation) = self._serialise_numpy_array(
                    datapart,
                    {},
                    serialisation
                )

                if serialisation in ["json"]:
                    serialised_shape = self._bytes_encode_check(
                        serialised_shape
                    )
                    serialised_dtype = self._bytes_encode_check(
                        serialised_dtype
                    )

                payload.append(serialised_array)
                payload.append(serialised_shape)
                payload.append(serialised_dtype)

            else:
                serialised_datapart, serialisation = self._serialise(
                    datapart,
                    serialisation
                )

                if serialisation in ["json"]:
                    serialised_datapart = self._bytes_encode_check(
                        serialised_datapart
                    )

                payload.append(serialised_datapart)
                metadata['_dataset_type'].append(None)

        serialised_metadata, serialisation = self._serialise(
            metadata,
            serialisation
        )

        if serialisation in ["json"]:
            serialised_metadata = self._bytes_encode_check(serialised_metadata)

        serialisation = self._bytes_encode_check(serialisation)

        # Connected to a proxy connection.
        if self._proxy_connection and self._sender_address is not None:
            self._connection.send_multipart(
                [
                    self._sender_address,
                    ''.encode(),
                    topic,
                    serialisation,
                    serialised_metadata
                ]
                + payload
            )

        else:
            self._connection.send_multipart(
                [
                    topic,
                    serialisation,
                    serialised_metadata
                ]
                + payload
            )

    #####################
    # _send_numpy_array #
    #####################

    def _send_numpy_array(
            self,
            array,
            metadata=None,
            topic=None,
            serialisation=None):

        if topic is None:
            topic = ''

        topic = self._bytes_encode_check(topic)

        (serialised_array,
            serialised_shape,
            serialised_dtype,
            serialised_metadata,
            serialisation) = self._serialise_numpy_array(
            array,
            metadata,
            serialisation
        )

        if serialisation in ["json"]:
            serialised_shape = self._bytes_encode_check(serialised_shape)
            serialised_dtype = self._bytes_encode_check(serialised_dtype)
            serialised_metadata = self._bytes_encode_check(serialised_metadata)

        serialisation = self._bytes_encode_check(serialisation)

        # Connected to a proxy connection.
        if self._proxy_connection and self._sender_address is not None:
            if self.verbosity > 4:
                print("Sending via a proxy")
            self._connection.send_multipart(
                [
                    self._sender_address,
                    b'',
                    topic,
                    serialised_array,
                    serialised_shape,
                    serialised_dtype,
                    serialisation,
                    serialised_metadata
                ]
            )

        else:
            self._connection.send_multipart(
                [
                    topic,
                    serialised_array,
                    serialised_shape,
                    serialised_dtype,
                    serialisation,
                    serialised_metadata
                ]
            )

    ##############
    # _serialise #
    ##############

    def _serialise(self, data, serialisation=None):
        '''
        Serialises the data.

        Parameters
        ----------
        data : any
            Data to serialise.

        serialisation : str, optional
            Serialisation protocol to use.
            Default is json.

        Returns
        -------
        any
            The serialised data.

        dict
            The metadata dict.
        '''

        if self.serialisation is not None:
            serialisation = self.serialisation

        # Default
        if serialisation is None:
            serialisation = self._defaults['serialisation']

        if serialisation not in _serialisation_schemes:
            raise ValueError(
                'Serialisation scheme not supported.'
            )

        if serialisation == 'json':
            if isinstance(data, numpy.dtype):
                data = str(data)

            serialised_data = json.dumps(data)

        elif serialisation == 'msgpack':
            if isinstance(data, numpy.dtype):
                data = str(data)

            # use_bin_type option is only needed for msgpack < 1.0.0
            # version > have this as a default option.
            serialised_data = msgpack.packb(
                data,
                use_bin_type=True
            )

        elif serialisation == 'pickle':
            serialised_data = pickle.dumps(data)

        return serialised_data, serialisation

    ##########################
    # _serialise_numpy_array #
    ##########################

    def _serialise_numpy_array(self, array, metadata, serialisation):
        '''
        Serialises a numpy array.

        Parameters
        ----------
        array : ndarray
            The array to serialise.

        metadata : dict
            Dictionary containing metadata.

        serialisation : str
            The serialisation scheme to use.

        Returns
        -------
        ndarray
        '''
        # serialised_array, serialisation = self._serialise(
        #     array,
        #     serialisation=serialisation
        # )

        serialised_shape, serialisation = self._serialise(
            array.shape,
            serialisation=serialisation
        )

        serialised_dtype, serialisation = self._serialise(
            array.dtype,
            serialisation=serialisation
        )

        serialised_metadata, serialisation = self._serialise(
            metadata,
            serialisation=serialisation
        )

        return (
            array,
            serialised_shape,
            serialised_dtype,
            serialised_metadata,
            serialisation
        )

    ########################
    # _set_high_water_mark #
    ########################

    def _set_high_water_mark(self, hwm):
        '''
        Sets the high water mark of the connection.

        Parameters
        ----------
        hwm : int
            The high water mark level.
        '''

        self._connection.set_hwm(hwm)

    ####################
    # _set_linger_time #
    ####################

    def _set_linger_time(self, linger):
        '''
        Sets the linger time of the connection.

        Parameters
        ----------
        linger : float
            The linger time of the connection in seconds.
        '''

        if linger is not None:
            self._linger = linger * 1000
        else:
            self._linger = self._defaults['linger'] * 1000

    ######################
    # _set_socket_option #
    ######################

    def _set_socket_option(self, option, value):
        '''
        Sets a socket option.

        Parameters
        ----------
        option : str
            The option to set.

        value : any
            The value the option to set to.
        '''

        self._connection.setsockopt(option, value.encode("utf-8"))

    ##########################
    # get_connection_details #
    ##########################

    def get_connection_details(self):
        '''
        Returns the current binding address.

        Returns
        -------
        str
            The port protocol.

        str
            The IP address.

        int
            The binding port.
        '''
        tmp = self._connection.getsockopt(zmq.LAST_ENDPOINT)
        if isinstance(tmp, bytes):
            tmp = tmp.decode('utf-8')
        tmp = tmp.replace('/', '')
        protocol, address, port = tmp.split(':')
        # print("type: {}".format(type(protocol)))

        # When auto port the address needs to be changed
        if address == '0.0.0.0' or address.startswith("127."):
            # address = socket.gethostbyname(socket.gethostname())
            address = socket.getfqdn()

        return [protocol, address, int(port)]

    #######################
    # get_connection_mode #
    #######################

    def get_connection_mode(self):
        '''
        Returns the mode of the connection, being client or server.

        Returns
        -------
        str
            The mode of the connection.
        '''
        return self._connection_mode

    ###########################
    # get_proxy_configuration #
    ###########################

    def get_proxy_configuration(self):
        '''
        Returns if the connection is configured for a proxy connection.

        Returns
        -------
        boolean
            Connection configured for proxy connection.
        '''
        return self._proxy_connection

    ###########################
    # set_proxy_configuration #
    ###########################

    def set_proxy_configuration(self, state):
        '''
        Sets if the connection is configured for a proxy connection.

        Parameters
        -------
        state : boolean
            Connection configured for proxy connection.
        '''

        self._proxy_connection = state

    ###########################
    # set_proxy_configuration #
    ###########################

    def send_proxy_backend_announcement(self):
        '''
        Sends out an announcement to a proxy.

        Used to announce the availability to the proxy backend.
        '''

        # self._connection.send(self._BE_READY)
        self.send_message(self._BE_READY)

    ########
    # stop #
    ########

    def stop(self, linger=None):
        '''
        Closes the socket and terminates the context.

        Parameters
        ----------
        linger : float
        '''

        if linger is not None:
            self._set_linger_time(linger)

        if hasattr(self, '_connection'):
            self._connection.close(linger=self._linger)
            self._context.term()


################
# _ZeroMQProxy #
################

class _ZeroMQProxy(_ZeroMQ):
    '''
    Base class for Proxies.
    '''

    ############
    # __init__ #
    ############

    def __init__(self, verbosity=0):
        super(_ZeroMQProxy, self).__init__(
            verbosity=verbosity
        )
        '''
        This base class contains the basics for a proxy.

        Parameters
        ----------
        verbosity : int, optional
            The verbosity level.
        '''
        self._stop = False

    ###############
    # _init_queue #
    ###############

    def _init_queue(self):

        return queue.Queue()

    #######################
    # _init_router_socket #
    #######################

    def _init_router_socket(self, port=None, protocol=None):
        '''
        Base class that provides the basic parts for a proxy.
        '''

        # self._queue = Queue.Queue()

        self._bind(
            'localhost',
            port,
            protocol,
            zmq.ROUTER
        )

        self.poller = self._poller_init('pollin')

    ############
    # _stopped #
    ############

    def _stopped(self):
        '''
        Returns the stop status.

        Returns
        -------
        boolean
            The stop status.
        '''

        return self._stop

    ##################
    # check_incoming #
    ##################

    def check_incoming(self, timeout=None):
        '''
        Checks for incoming messages.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.
        '''

        return self._poller_check(self.poller, timeout)

    ###################
    # receive_message #
    ###################

    def receive_message(self):
        '''
        Receive a message.
        '''

        return self._connection.recv_multipart()

    ################
    # send_message #
    ################

    def send_message(self, msg, addr=None):
        '''
        Send a message.

        Parameters
        ----------
        msg : any
            The message to send.

        addr : str, optional
            The address to add to the message.
        '''

        if addr is not None:
            msg = [addr, b""] + msg

        self._connection.send_multipart(msg)

    ########
    # stop #
    ########

    def stop(self):
        '''
        Stops the proxy.
        '''

        self._stop = True


################
# _ZeroMQQueue #
################

class _ZeroMQQueue(threading.Thread):
    '''
    Class for shared methods for ZMQ Queues.
    '''

    ############
    # __init__ #
    ############

    def __init__(
            self,
            queue_type):
        '''
        Parameters
        ----------
        queue_type : str
            The queue type.
        '''

        super(_ZeroMQQueue, self).__init__()
        self._queue_type = queue_type
        self._stop = False

        if self._queue_type == 'fifo':
            self._queue = queue.Queue()
        elif self._queue_type == 'lifo':
            self._queue = queue.LifoQueue()
        elif self._queue_type == 'priority_queue':
            self._queue = queue.PriorityQueue()

    ############
    # _stopped #
    ############

    def _stopped(self):
        '''
        Return stopped status.
        '''

        return self._stop

    ###############
    # clear_queue #
    ###############

    def clear_queue(self):
        '''
        Clears the content of the queue.
        '''
        with self._queue.mutex:
            if self._queue_type == 'fifo':
                self._queue.queue.clear()
            else:
                try:
                    while True:
                        self._queue.queue.pop(0)
                except IndexError:
                    pass

    ##########################
    # get_connection_details #
    ##########################

    def get_connection_details(self):
        '''
        Returns the current binding address.

        Returns
        -------
        str
            The port protocol.

        str
            The IP address.

        int
            The binding port.
        '''

        protocol_in, address_in, port_in = (
            self.receiver.get_connection_details()
        )

        protocol_out, address_out, port_out = (
            self.sender.get_connection_details()
        )

        ret_val = (
            (
                protocol_in,
                address_in,
                port_in
            ),
            (
                protocol_out,
                address_out,
                port_out
            ),
        )

        return ret_val

    ##############
    # queue_size #
    ##############

    def queue_size(self):
        '''
        Returns the size of the queue.

        Returns
        -------
        int
            The size of the queue.
        '''
        return self._queue.qsize()

    ##############
    # queue_type #
    ##############

    def queue_type(self):
        '''
        Returns the type of the queue.

        Returns
        -------
        str
            The type of the queue.
        '''
        return self._queue_type

    ########
    # stop #
    ########

    def stop(self):
        '''
        Method to stop the thread.
        '''

        self._stop = True
        if self.verbosity > 5:
            print('Stop set.')


#############
# ClientPub #
#############

class ClientPub(_ZeroMQ):
    def __init__(
            self,
            host,
            port,
            hwm=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Publication client.

        Parameters
        ----------
        host : str
            Host name of the server.

        port : int
            Port number the server is running on.

        hwm : int, optional
            High water mark.
            Default is None.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbosity level.
            Default is 0.

        Notes
        -----
        When a ClientPub is started, it is unable to announce itself with the
        ServerSub. Therefore all message will be lost until at least 1 message
        has been sent by ClientPub and the ServerSub has gone through a
        message receive cycle.
        '''

        super(ClientPub, self).__init__(
            serialisation=serialisation,
            verbosity=verbosity
        )

        self._connect(host, port, protocol, zmq.PUB)

        # Set the High Water Mark
        time.sleep(0.2)
        if hwm is not None:
            self._set_high_water_mark(hwm)

    ############
    # announce #
    ############

    def announce(self, topic=None):
        '''
        Announces the publisher to the ServerSub upon start.
        Only when the ServerSub has gone through a receive cycle, the messages
        from the ClientSub will be delivered on the ServerSub side.
        '''

        self.send_message('', topic=topic)

    ################
    # send_message #
    ################

    def send_message(self, message, topic=None):
        '''
        Send a message.

        Parameters
        ----------
        message : str
            The message to send.

        topic : str, optional
            The topic of the message.
            If no topic has been provided an empty topic will be used.
        '''

        self._send_message(
            message,
            topic=topic
        )

    ####################
    # send_numpy_array #
    ####################

    def send_numpy_array(self, array, topic=None, metadata=None):
        '''
        Sends a numpy array.

        Parameters
        ----------
        array : ndarray
            The array to send.

        topic : str
            The topic of the message.

        metadata : dict, optional
            Dict containing extra metadata.
        '''

        self._send_numpy_array(
            array,
            metadata=metadata,
            topic=topic
        )


##############
# ClientPull #
##############

class ClientPull(_ZeroMQ):

    def __init__(
            self,
            host,
            port,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Pull client.

        Parameters
        ----------
        host : str
            Host name of the server.

        port : int
            Port number the server is running on.

        hwm : int, optional
            High water mark.
            Default is None.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ClientPull, self).__init__(
            serialisation=serialisation,
            verbosity=verbosity
        )

        self._connect(host, port, protocol, zmq.PULL)

        self.poller = self._poller_init('pollin')

    ##################
    # check_incoming #
    ##################

    def check_incoming(self, timeout=None):
        '''
        Checks for incoming messages.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.
        '''

        return self._poller_check(self.poller, timeout)

    ###################
    # receive_message #
    ###################

    def receive_message(self, timeout=None):
        '''
        Put the client in the receive message state until the time out expires.

        Parameters
        ----------
        timeout : float
            The time to wait for a message.
            Time in seconds.

        Returns
        -------
        any
            The message that was received.
        '''

        if self.check_incoming(timeout):
            _, msg, _ = self._receive_message()
        else:
            msg = None

        return msg

    #########################
    # receive_multi_dataset #
    #########################

    def receive_multi_dataset(self, timeout=None):
        '''
        Receive a multiple dataset message.

        Parameters
        ----------
        timeout : float, optional
            The timeout in seconds to wait for a message.

        Returns
        -------
        list
            List containing the datasets.

        dict
            Attached metadata.
        '''

        if self.check_incoming(timeout):
            topic, data, metadata = self._receive_multi()
            return data, metadata
        else:
            return None

    #######################
    # receive_numpy_array #
    #######################

    def receive_numpy_array(self, timeout=None):
        '''
        Receives a numpy array.

        Parameters
        ----------
        timeout : scalar (float)
            Timeout in seconds

        Returns
        -------
        array : ndarray
            Numpy array

        metadata : dict
            Metadata dictionary
        '''

        if self.check_incoming(timeout):
            _, array, metadata = self._receive_numpy_array()

            return array, metadata
        else:
            return None


##############
# ClientPush #
##############

class ClientPush(_ZeroMQ):

    ############
    # __init__ #
    ############

    def __init__(
            self,
            host,
            port,
            hwm=None,
            linger=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Push client.

        Parameters
        ----------
        host : str
            Host name of the server.

        port : int
            Port number the server is running on.

        hwm : int, optional
            High water mark.
            Default is None.

        linger : float, optional
            Linger time in s before closing the socket.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ClientPush, self).__init__(
            linger=linger,
            serialisation=serialisation,
            verbosity=verbosity
        )

        self._connect(host, port, protocol, zmq.PUSH)

        # Set the High Water Mark
        time.sleep(0.2)
        if hwm is not None:
            self._set_high_water_mark(hwm)

    ################
    # send_message #
    ################

    def send_message(self, message):
        '''
        Send a message.

        Parameters
        ----------
        message : str
            The message to send.
        '''

        # _message = pickle.dumps(message)
        # self.client.send(_message)
        self._send_message(message)

        return 1

    ######################
    # send_multi_dataset #
    ######################

    def send_multi_dataset(self, data, metadata=None):
        '''
        Sends out a multiple datasets.

        Parameters
        ----------
        data : list
            A list containing the datasets you want to send.

        metadata : dict, optional
            Extra possible metadata.
        '''

        self._send_multi(data, metadata=metadata)

    ####################
    # send_numpy_array #
    ####################

    def send_numpy_array(self, array, metadata=None):
        '''
        Sends a numpy array.
        '''

        self._send_numpy_array(
            array,
            metadata=metadata
        )


#############
# ClientRep #
#############

class ClientRep(_ZeroMQ):

    ############
    # __init__ #
    ############

    def __init__(
            self,
            host,
            port,
            linger=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Reply client.

        Parameters
        ----------
        host : str
            Host name of the server.

        port : int
            Port number the server is running on.

        linger : float, optional
            Linger time in s before closing the socket.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ClientRep, self).__init__(
            linger=linger,
            serialisation=serialisation,
            verbosity=verbosity
        )

        self._connect(host, port, protocol, zmq.REP)

        self.poller_in = self._poller_init('pollin')
        self.poller_out = self._poller_init('pollout')

    ##################
    # check_incoming #
    ##################

    def check_incoming(self, timeout=None):
        '''
        Checks for incoming messages.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.

        Returns
        -------
        boolean
            True when a message is available.
            False when no message is available.
        '''

        return self._poller_check(self.poller_in, timeout)

    ##################
    # check_outgoing #
    ##################

    def check_outgoing(self, timeout=None):
        '''
        Checks for confirmation if a message can be send.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.

        Returns
        -------
        boolean
            True when a message can be send.
            False when the message can not ne send.
        '''

        return self._poller_check(self.poller_out, timeout)

    ###################
    # receive_message #
    ###################

    def receive_message(self, timeout=None):
        '''
        Put the client in the receive message state.
        '''

        if self.check_incoming(timeout):
            _, msg, _ = self._receive_message()
        else:
            msg = None

        return msg

    #######################
    # receive_numpy_array #
    #######################

    def receive_numpy_array(self, timeout=None):
        '''
        Receives a numpy array.

        Parameters
        ----------
        timeout : scalar (float)
            Timeout in seconds

        Returns
        -------
        array : ndarray
            Numpy array

        metadata : dict
            Metadata dictionary

        If no message was received within the timeout, None is returned.
        '''

        if self.check_incoming(timeout):
            topic, array, metadata = self._receive_numpy_array()
            return array, metadata
        else:
            return None

    ################
    # send_message #
    ################

    def send_message(self, message, timeout=None):
        '''
        Send a message.

        Parameters
        ----------
        message : str
            The message to send.

        timeout : float
            The time to wait for a message.
            Time in seconds.
        '''

        if self.check_outgoing(timeout):
            self._send_message(message)
            return 1
        else:
            return None

    ######################
    # send_multi_dataset #
    ######################

    def send_multi_dataset(self, data, metadata=None):
        '''
        Sends out a multiple datasets.

        Parameters
        ----------
        data : list
            A list containing the datasets you want to send.

        metadata : dict, optional
            Extra possible metadata.
        '''

        self._send_multi(data, metadata=metadata)

    ####################
    # send_numpy_array #
    ####################

    def send_numpy_array(self, array, topic=None, metadata=None):
        '''
        Sends a numpy array.

        Parameters
        ----------
        array : ndarray
            The array to send.

        topic : str
            The topic of the message.

        metadata : dict, optional
            Dict containing extra metadata.
        '''

        self._send_numpy_array(
            array,
            metadata=metadata,
            topic=topic
        )


#############
# ClientReq #
#############

class ClientReq(_ZeroMQ):

    def __init__(
            self,
            host,
            port,
            linger=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Request client.

        Parameters
        ----------
        host : str
            Host name of the server.

        port : int
            Port number the server is running on.

        linger : float, optional
            Linger time in s before closing the socket.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ClientReq, self).__init__(
            linger=linger,
            serialisation=serialisation,
            verbosity=verbosity
        )

        self._connect(host, port, protocol, zmq.REQ)

        self.poller_in = self._poller_init('pollin')
        self.poller_out = self._poller_init('pollout')

    ##################
    # check_incoming #
    ##################

    def check_incoming(self, timeout=None):
        '''
        Checks for incoming messages.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.

        Returns
        -------
        boolean
            True when a message is available.
            False when no message is available.
        '''

        return self._poller_check(self.poller_in, timeout)

    ##################
    # check_outgoing #
    ##################

    def check_outgoing(self, timeout=None):
        '''
        Checks for confirmation if a message can be send.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.

        Returns
        -------
        boolean
            True when a message can be send.
            False when the message can not ne send.
        '''

        return self._poller_check(self.poller_out, timeout)

    ###################
    # receive_message #
    ###################

    def receive_message(self, timeout=None):
        '''
        Put the client in the receive message state.
        '''

        if self.check_incoming(timeout):
            _, msg, _ = self._receive_message()
        else:
            msg = None

        return msg

    #########################
    # receive_multi_dataset #
    #########################

    def receive_multi_dataset(self, timeout=None):
        '''
        Receive a multiple dataset message.

        Parameters
        ----------
        timeout : float, optional
            The timeout in seconds to wait for a message.

        Returns
        -------
        list
            List containing the datasets.

        dict
            Attached metadata.
        '''

        if self.check_incoming(timeout):
            topic, data, metadata = self._receive_multi()
            return data, metadata
        else:
            return None

    #######################
    # receive_numpy_array #
    #######################

    def receive_numpy_array(self, timeout=None):
        '''
        Receives a numpy array.

        Parameters
        ----------
        timeout : scalar (float)
            Timeout in seconds

        Returns
        -------
        array : ndarray
            Numpy array

        metadata : dict
            Metadata dictionary
        '''

        if self.check_incoming(timeout):
            _, array, metadata = self._receive_numpy_array()
            return array, metadata
        else:
            return None

    ################
    # send_message #
    ################

    def send_message(self, message, timeout=None):
        '''
        Send a message.

        Parameters
        ----------
        message : str
            The message to send.

        timeout : float
            The time to wait for a message.
            Time in seconds.
        '''

        if self.check_outgoing(timeout):
            self._send_message(message)
            return 1
        else:
            return None

    ######################
    # send_multi_dataset #
    ######################

    def send_multi_dataset(self, data, metadata=None):
        '''
        Sends out a multiple datasets.

        Parameters
        ----------
        data : list
            A list containing the datasets you want to send.

        metadata : dict, optional
            Extra possible metadata.
        '''

        self._send_multi(data, metadata=metadata)

    ####################
    # send_numpy_array #
    ####################

    def send_numpy_array(self, array, metadata=None):
        '''
        Sends a numpy array.

        Parameters
        ----------
        array : ndarray
            The array to send.

        metadata : dict, optional
            Extra metadata to send.
        '''

        self._send_numpy_array(
            array,
            metadata=metadata
        )

    ########################
    # send_receive_message #
    ########################

    def send_receive_message(self, message, timeout=None):
        '''
        Send a message.

        Parameters
        ----------
        message : str
            The message to send.

        timeout : float
            The time to wait for a message.
            Time in seconds.
        '''

        _tmp = self.send_message(message, timeout)
        if _tmp is not None:
            return self.receive_message(timeout)
        else:
            return None


#############
# ClientSub #
#############

class ClientSub(_ZeroMQ):
    def __init__(
            self,
            host,
            port,
            hwm=None,
            topic=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Sub client.

        Parameters
        ----------
        host : str
            Host name of the server.

        port : int
            Port number the server is running on.

        hwm : int, optional
            High water mark.
            Default is None.

        topic : List of str, optional
            List of topics to which to subscribe to.
            Default subscription is to all published messages.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ClientSub, self).__init__(
            serialisation=serialisation,
            verbosity=verbosity
        )

        self._init_connection_info = {
            "args": [host, port, protocol, zmq.SUB],
            "kwargs": {
                "hwm": hwm
            }
        }

        if topic is None:
            topic = ['']

        # Set the subscription
        if isinstance(topic, str):
            topic = [topic]
        if PYTHON_VERSION == 2:
            if isinstance(topic, unicode):  # noqa F821
                topic = [topic]

        self._init_connection(topic)

    ####################
    # _init_connection #
    ####################

    def _init_connection(self, topics):

        # self._connect(host, port, protocol, zmq.SUB, hwm=hwm)
        self._connect(
            *self._init_connection_info["args"],
            **self._init_connection_info["kwargs"]
        )

        for topic in topics:
            self.remove_subscription_topic(topic)
            self.add_subscription_topic(topic)

        self.poller = self._poller_init('pollin')

    ##########################
    # add_subscription_topic #
    ##########################

    def add_subscription_topic(self, topic):
        '''
        Adds a subscription topic to the socket.

        Parameters
        ----------
        topic : str
            topic subscription to add to the socket.
        '''

        self._add_subscription_topic(topic)

        return 1

    ##################
    # check_incoming #
    ##################

    def check_incoming(self, timeout=None):
        '''
        Checks for incoming messages.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.
        '''

        return self._poller_check(self.poller, timeout)

    ###########################
    # get_subscription_topics #
    ###########################

    def get_subscription_topics(self):
        '''
        Returns the subscription topics of the socket.

        Returns
        ----------
        list
            List containing the subscription topics of the socket.
        '''

        print("sub topics: {}".format(self._subscription_list))
        return self._subscription_list

    ###################
    # receive_message #
    ###################

    def receive_message(self, timeout=None):
        '''
        Put the client in the receive message state.

        Parameters
        ----------
        timeout : float
            The timeout (in seconds) to wait for a message.
            By default the methods waits forever.

        Returns
        -------
        any
            The message received message.
            The data type depends on what was received.

        str
            The topic of the message that was received.
        '''

        if self.check_incoming(timeout):
            topic, msg, _ = self._receive_message()
            return msg, topic
        else:
            return None

    #######################
    # receive_numpy_array #
    #######################

    def receive_numpy_array(self, timeout=None):
        '''
        Receives a numpy array.

        Parameters
        ----------
        timeout : scalar (float)
            Timeout in seconds

        Returns
        -------
        array : ndarray
            Numpy array

        metadata : dict
            Metadata dictionary

        If no message was received within the timeout, None is returned.
        '''

        if self.check_incoming(timeout):
            topic, array, metadata = self._receive_numpy_array()
            return array, metadata, topic
        else:
            return None

    #############
    # reconnect #
    #############

    def reconnect(self):
        """
        Reconnects the Client.
        """

        self.stop()
        self._init_connection(self.get_subscription_topics())

    #############################
    # remove_subscription_topic #
    #############################

    def remove_subscription_topic(self, topic):
        '''
        Remove a subscription topic from the socket.

        Parameters
        ----------
        topic : str
            topic subscription to remove from the socket.
        '''

        self._remove_subscription_topic(topic)

        return 1


#######################
# connection_selector #
#######################

def connection_selector(
        conn_mode,
        conn_type,
        *args,
        **kwargs):
    '''
    ZeroMQ connection selector.

    Parameters
    ----------
    conn_mode : str
        The connection mode, being server or client.

    conn_type : str
        The connection type: 'pub', 'pull', 'push', 'rep', 'req', 'sub'

    *args
        Extra needed arguments for the connection if needed.

    **kwargs
        Other available options for the connection.

    Returns
    -------
    instance
        The connection instance.
    '''

    if conn_mode == 'client':
        if conn_type == 'pub':
            return ClientPub(
                *args,
                **kwargs
            )
        elif conn_type == 'pull':
            return ClientPull(
                *args,
                **kwargs
            )
        elif conn_type == 'push':
            return ClientPush(
                *args,
                **kwargs
            )
        elif conn_type == 'rep':
            return ClientRep(
                *args,
                **kwargs
            )
        elif conn_type == 'req':
            return ClientReq(
                *args,
                **kwargs
            )
        elif conn_type == 'sub':
            return ClientSub(
                *args,
                **kwargs
            )
        else:
            raise ValueError('Unsupported connection type.')

    elif conn_mode == 'server':
        if conn_type == 'pub':
            return ServerPub(**kwargs)
        elif conn_type == 'pull':
            return ServerPull(**kwargs)
        elif conn_type == 'push':
            return ServerPush(**kwargs)
        elif conn_type == 'rep':
            return ServerRep(**kwargs)
        elif conn_type == 'req':
            return ServerReq(**kwargs)
        elif conn_type == 'sub':
            return ServerSub(**kwargs)
        else:
            raise ValueError('Unsupported connection type.')

    else:
        raise ValueError('Unsupported connection mode.')


#############
# ServerPub #
#############

class ServerPub(_ZeroMQ):
    def __init__(
            self,
            port=None,
            hwm=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Pub server.

        Parameters
        ----------
        port : int, optional
            Port number the server is running on.

        hwm : int, optional
            High water mark.
            Default is None.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ServerPub, self).__init__(
            serialisation=serialisation,
            verbosity=verbosity
        )

        host = 'localhost'

        self._bind(
            host,
            port,
            protocol,
            zmq.PUB,
            hwm=hwm
        )

    ################
    # send_message #
    ################

    def send_message(self, message, topic=None):
        '''
        Send a message.

        Parameters
        ----------
        message : str
            The message to send.

        topic : str, optional
            The topic of the message.
            If no topic has been provided an empty topic will be used.
        '''

        self._send_message(
            message,
            topic=topic
        )

    ####################
    # send_numpy_array #
    ####################

    def send_numpy_array(self, array, topic=None, metadata=None):
        '''
        Sends a numpy array.

        Parameters
        ----------
        array : ndarray
            The array to send.

        topic : str
            The topic of the message.

        metadata : dict, optional
            Dict containing extra metadata.
        '''

        self._send_numpy_array(
            array,
            metadata=metadata,
            topic=topic
        )


##############
# ServerPull #
##############

class ServerPull(_ZeroMQ):

    def __init__(
            self,
            port=None,
            hwm=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Pull server.

        Parameters
        ----------
        port : int
            Port number the server is running on.

        hwm : int, optional
            High water mark.
            Default is None.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ServerPull, self).__init__(
            serialisation=serialisation,
            verbosity=verbosity
        )

        host = 'localhost'
        self._bind(
            host,
            port,
            protocol,
            zmq.PULL,
            hwm=hwm
        )

        self.poller = self._poller_init('pollin')

    ##################
    # check_incoming #
    ##################

    def check_incoming(self, timeout=None):
        '''
        Checks for incoming messages.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.
        '''

        return self._poller_check(self.poller, timeout)

    ###################
    # receive_message #
    ###################

    def receive_message(self, timeout=None):
        '''
        Put the server in the receive message state.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.

        Returns
        -------
        any
            Message content.
            If no message was received within the timeout, None is returned.
        '''

        if self.check_incoming(timeout):
            _, msg, _ = self._receive_message()

            # if type(msg) == unicode:
            #     msg = str(msg)

            return msg
        else:
            return None

    #########################
    # receive_multi_dataset #
    #########################

    def receive_multi_dataset(self, timeout=None):
        '''
        Receive a multiple dataset message.

        Parameters
        ----------
        timeout : float, optional
            The timeout in seconds to wait for a message.

        Returns
        -------
        list
            List containing the datasets.

        dict
            Attached metadata.
        '''

        if self.check_incoming(timeout):
            topic, data, metadata = self._receive_multi()
            return data, metadata
        else:
            return None

    #######################
    # receive_numpy_array #
    #######################

    def receive_numpy_array(self, timeout=None):
        '''
        Receives a numpy array.

        Parameters
        ----------
        timeout : scalar (float)
            Timeout in seconds

        Returns
        -------
        array : ndarray
            Numpy array

        metadata : dict
            Metadata dictionary
        '''

        if self.check_incoming(timeout):
            _, array, metadata = self._receive_numpy_array()
            return array, metadata
        else:
            return None


##############
# ServerPush #
##############

class ServerPush(_ZeroMQ):

    ############
    # __init__ #
    ############

    def __init__(
            self,
            port=None,
            hwm=None,
            linger=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Push server.

        Parameters
        ----------
        port : int, optional
            Port number the server is running on.

        hwm : int, optional
            High water mark.
            Default is None.

        linger : float, optional
            Linger time in s before closing the socket.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbosity level.
            Default is 0.
        '''

        super(ServerPush, self).__init__(
            linger=linger,
            serialisation=serialisation,
            verbosity=verbosity
        )

        host = 'localhost'
        self._bind(
            host,
            port,
            protocol,
            zmq.PUSH,
            hwm=hwm
        )

    ################
    # send_message #
    ################

    def send_message(self, message, serialisation=None):
        '''
        Send a message.

        Parameters
        ----------
        message : any
            The message you want to send.

        serialisation : str, optional
            The serialisation type to use.
        '''

        self._send_message(message, serialisation=serialisation)

        return 1

    ######################
    # send_multi_dataset #
    ######################

    def send_multi_dataset(self, data, metadata=None):
        '''
        Sends out a multiple datasets.

        Parameters
        ----------
        data : list
            A list containing the datasets you want to send.

        metadata : dict, optional
            Extra possible metadata.
        '''

        self._send_multi(data, metadata=metadata)

    ####################
    # send_numpy_array #
    ####################

    def send_numpy_array(self, array, metadata=None):
        '''
        Sends a numpy array.

        Parameters
        ----------
        array : ndarray
            The array to send.

        metadata : dict, optional
            Dict containing extra metadata.
        '''

        self._send_numpy_array(array, metadata=metadata)


#############
# ServerRep #
#############

class ServerRep(_ZeroMQ):

    def __init__(
            self,
            port=None,
            linger=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Rep server.

        Parameters
        ----------
        port : int, optional
            Port number the server is running on.

        linger : float, optional
            Linger time in s before closing the socket.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ServerRep, self).__init__(
            linger=linger,
            serialisation=serialisation,
            verbosity=verbosity
        )
        host = 'localhost'
        self._bind(
            host,
            port,
            protocol,
            zmq.REP
        )

        self.poller_in = self._poller_init('pollin')
        self.poller_out = self._poller_init('pollout')

    ##################
    # check_incoming #
    ##################

    def check_incoming(self, timeout=None):
        '''
        Checks for incoming messages.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.
        '''

        return self._poller_check(self.poller_in, timeout)

    ##################
    # check_outgoing #
    ##################

    def check_outgoing(self, timeout=None):
        '''
        Checks for confirmation if a message can be send.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.

        Returns
        -------
        boolean
            True when a message can be send.
            False when the message can not ne send.
        '''

        return self._poller_check(self.poller_out, timeout)

    ###################
    # receive_message #
    ###################

    def receive_message(self, timeout=None):
        '''
        Put the server in the receive message state.

        Parameters
        ----------
        timeout : float
            The time to wait for a message.
            Time in seconds.
        '''

        if self.check_incoming(timeout):
            _, msg, _ = self._receive_message()
        else:
            msg = None

        return msg

    #########################
    # receive_multi_dataset #
    #########################

    def receive_multi_dataset(self, timeout=None):
        '''
        Receive a multiple dataset message.

        Parameters
        ----------
        timeout : float, optional
            The timeout in seconds to wait for a message.

        Returns
        -------
        list
            List containing the datasets.

        dict
            Attached metadata.
        '''

        if self.check_incoming(timeout):
            _, data, metadata = self._receive_multi()
            return data, metadata
        else:
            return None

    #######################
    # receive_numpy_array #
    #######################

    def receive_numpy_array(self, timeout=None):
        '''
        Receives a numpy array.

        Parameters
        ----------
        timeout : scalar (float)
            Timeout in seconds

        Returns
        -------
        array : ndarray
            Numpy array

        metadata : dict
            Metadata dictionary

        If no message was received within the timeout, None is returned.
        '''

        if self.check_incoming(timeout):
            topic, array, metadata = self._receive_numpy_array()
            return array, metadata
        else:
            return None

    ################
    # send_message #
    ################

    def send_message(self, message, timeout=None):
        '''
        Send a message.

        Parameters
        ----------
        message : str
            The message to send.

        timeout : float
            The time to wait for a message.
            Time in seconds.
        '''

        if self.check_outgoing(timeout):
            self._send_message(message)
            return 1
        else:
            return None

    ######################
    # send_multi_dataset #
    ######################

    def send_multi_dataset(self, data, metadata=None):
        '''
        Sends out a multiple datasets.

        Parameters
        ----------
        data : list
            A list containing the datasets you want to send.

        metadata : dict, optional
            Extra possible metadata.
        '''

        self._send_multi(data, metadata=metadata)

    ####################
    # send_numpy_array #
    ####################

    def send_numpy_array(self, array, metadata=None, timeout=None):
        '''
        Sends a numpy array.

        array : ndarray
            The array to send.

        metadata : dict, optional
            Dict containing extra metadata.

        timeout : float
            The time to wait for a message.
            Time in seconds.
        '''

        if self.check_outgoing(timeout=None):
            self._send_numpy_array(
                array,
                metadata=metadata
            )


#############
# ServerReq #
#############

class ServerReq(_ZeroMQ):

    def __init__(
            self,
            port=None,
            linger=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Request server.

        Parameters
        ----------
        port : int, optional
            Port number the server is running on.

        linger : float, optional
            Linger time in s before closing the socket.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ServerReq, self).__init__(
            linger=linger,
            serialisation=serialisation,
            verbosity=verbosity
        )
        host = 'localhost'
        self._bind(
            host,
            port,
            protocol,
            zmq.REQ
        )

        self.poller_in = self._poller_init('pollin')
        self.poller_out = self._poller_init('pollout')

    ##################
    # check_incoming #
    ##################

    def check_incoming(self, timeout=None):
        '''
        Checks for incoming messages.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.
        '''

        return self._poller_check(self.poller_in, timeout)

    ##################
    # check_outgoing #
    ##################

    def check_outgoing(self, timeout=None):
        '''
        Checks for confirmation if a message can be send.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.

        Returns
        -------
        boolean
            True when a message can be send.
            False when the message can not ne send.
        '''

        return self._poller_check(self.poller_out, timeout)

    ###################
    # receive_message #
    ###################

    def receive_message(self, timeout=None):
        '''
        Put the server in the receive message state.

        Parameters
        ----------
        timeout : float
            The time to wait for a message.
            Time in seconds.
        '''

        if self.check_incoming(timeout):
            _, msg, _ = self._receive_message()
        else:
            msg = None

        return msg

    #########################
    # receive_multi_dataset #
    #########################

    def receive_multi_dataset(self, timeout=None):
        '''
        Receive a multiple dataset message.

        Parameters
        ----------
        timeout : float, optional
            The timeout in seconds to wait for a message.

        Returns
        -------
        list
            List containing the datasets.

        dict
            Attached metadata.
        '''

        if self.check_incoming(timeout):
            topic, data, metadata = self._receive_multi()
            return data, metadata
        else:
            return None

    #######################
    # receive_numpy_array #
    #######################

    def receive_numpy_array(self, timeout=None):
        '''
        Receives a numpy array.

        Parameters
        ----------
        timeout : scalar (float)
            Timeout in seconds

        Returns
        -------
        array : ndarray
            Numpy array

        metadata : dict
            Metadata dictionary

        If no message was received within the timeout, None is returned.
        '''

        if self.check_incoming(timeout):
            topic, array, metadata = self._receive_numpy_array()
            return array, metadata
        else:
            return None

    ################
    # send_message #
    ################

    def send_message(self, message, timeout=None):
        '''
        Send a message.

        Parameters
        ----------
        message : str
            The message to send.

        timeout : float
            The time to wait for a message.
            Time in seconds.
        '''

        if self.check_outgoing(timeout):
            self._send_message(message)
            return 1
        else:
            return None

    ####################
    # send_numpy_array #
    ####################

    def send_numpy_array(self, array, metadata=None):
        '''
        Sends a numpy array.

        Parameters
        ----------
        array : ndarray
            The array to send.

        metadata : dict, optional
            Extra metadata to send.
        '''

        self._send_numpy_array(
            array,
            metadata=metadata
        )

    ########################
    # send_receive_message #
    ########################

    def send_receive_message(self, message, timeout=None):
        '''
        Send a message.
        '''

        tmp = self.send_message(message, timeout)

        if tmp is not None:
            return self.receive_message(timeout)
        else:
            return None


#############
# ServerSub #
#############

class ServerSub(_ZeroMQ):

    def __init__(
            self,
            port=None,
            hwm=None,
            topic=None,
            protocol=None,
            serialisation=None,
            verbosity=0):
        '''
        Sub server.

        Parameters
        ----------
        port : int, optional
            Port number the server is running on.

        hwm : int, optional
            High water mark.
            Default is None.

        topic : list of str, optional
            List of topics to which to subscribe to.
            Default subscription is to all published messages.

        protocol : str, optional
            The protocol used by ZMQ.
            Default is tcp.

        serialisation : str, optional
            The serialisation method to use.

        verbosity : int, optional
            Verbose mode.
            Default is 0.
        '''

        super(ServerSub, self).__init__(
            serialisation=serialisation,
            verbosity=verbosity
        )

        host = 'localhost'
        self._bind(
            host,
            port,
            protocol,
            zmq.SUB,
            hwm=hwm
        )

        if topic is None:
            topic = ['']

        # Set the subscription
        if isinstance(topic, str):
            topic = [topic]
        if PYTHON_VERSION == 2:
            if isinstance(topic, unicode):  # noqa F821
                topic = [topic]

        for _topic in topic:
            self.add_subscription_topic(_topic)

        self.poller = self._poller_init('pollin')

    ##########################
    # add_subscription_topic #
    ##########################

    def add_subscription_topic(self, topic):
        '''
        Adds a subscription topic to the socket.

        Parameters
        ----------
        topic : str
            Topic subscription to add to the socket.
        '''

        self._add_subscription_topic(topic)

        return 1

    ##################
    # check_incoming #
    ##################

    def check_incoming(self, timeout=None):
        '''
        Checks for incoming messages.

        Parameters
        ----------
        timeout : float
            Timeout in seconds.
        '''

        return self._poller_check(self.poller, timeout)

    ###########################
    # get_subscription_topics #
    ###########################

    def get_subscription_topics(self):
        '''
        Returns the subscription topics of the socket.

        Returns
        ----------
        list
            List containing the subscription topics of the socket.
        '''

        return self._subscription_list

    ###################
    # receive_message #
    ###################

    def receive_message(self, timeout=None):
        '''
        Put the server in the receive message state.

        Returns
        -------
        any
            The message that was sent by the sender.
            The type is depending on what was sent.

        str
            The topic of the received message.
            If no message was received within the timeout, None is returned.
        '''

        if self.check_incoming(timeout):
            topic, msg, _ = self._receive_message()
            return msg, topic
        else:
            return None

    #######################
    # receive_numpy_array #
    #######################

    def receive_numpy_array(self, timeout=None):
        '''
        Receives a numpy array.

        Parameters
        ----------
        timeout : scalar (float)
            Timeout in seconds

        Returns
        -------
        array : ndarray
            Numpy array

        metadata : dict
            Metadata dictionary

        If no message was received within the timeout, None is returned.
        '''

        if self.check_incoming(timeout):
            topic, array, metadata = self._receive_numpy_array()
            return array, metadata, topic
        else:
            return None

    #############################
    # remove_subscription_topic #
    #############################

    def remove_subscription_topic(self, topic):
        '''
        Remove a subscription topic from the socket.

        Parameters
        ----------
        topic : str
            topic subscription to remove from the socket.
        '''

        self._remove_subscription_topic(topic)

        return 1


#############
# ZmqPoller #
#############

class ZmqPoller(object):

    ############
    # __init__ #
    ############

    def __init__(self, zmqSocket, pollerType):
        '''
        Initiates a poller.

        parameters
        ----------
        zmqSocket : instance
            Zmq socket that needs to be registered.
        pollerType : str
            Polling type: pollin || pollout
        '''

        self.zmqSocket = zmqSocket

        self.poller = zmq.Poller()
        if pollerType == 'pollin':
            self.pollerType = zmq.POLLIN
        elif pollerType == 'pollout':
            self.pollerType = zmq.POLLOUT

        self.poller.register(self.zmqSocket, self.pollerType)

    #########
    # check #
    #########

    def check(self, timeout=None):
        '''
        Check if new messages arrived on the socket.

        Parameters
        ----------
        timeout : float
            Timeout of the poller in seconds.
        '''

        # Convert s to ms
        # fixme: should be method
        if timeout is not None:
            timeout *= 1000

        socks = dict(self.poller.poll(timeout))

        if (self.zmqSocket in socks
                and socks[self.zmqSocket]) == self.pollerType:
            return True
        else:
            return False


#####################
# zmq_sub_rep_queue #
#####################

class ZmqSubRepQueue(_ZeroMQQueue):
    '''
    A ZMQ Subscription Reply Queue.
    '''
    ############
    # __init__ #
    ############

    def __init__(
            self,
            host_in=None,
            port_in=None,
            host_out=None,
            port_out=None,
            input_mode='client',
            output_mode='server',
            topic=None,
            queue_type='fifo',
            verbosity=0):
        '''
        A zmq enabled queue using a sub socket as input and a
        rep socket as output. Input and output can be set to server or client.

        Parameters
        ----------
        host_in : str, optional
            The input host to connect to when operated in client mode.

        port_in : int, optional
            The input socket port.
            In server mode, when None is provided, the system will allocate
            a free sockets automatically.

        host_out : str, optional
            The output host to connect to when operated in client mode.

        port_in : int, optional
            The output socket port.
            In server mode, when None is provided, the system will allocate
            a free sockets automatically.

        input_mode : str, optional
            The type of connection for the input.
            Client or Server.
            Default is Client.

        output_mode : str, optional
            The type of connection for the output.
            Client or Server.
            Default is Server.

        topic : str, optional
            The topic to subscribe to.
            Default is subscribe to all.

        queue_type : str, optional
            The type of queue to use.
            The following queues are available:
            + fifo (default)
            + lifo
            + priority_queue

        verbosity : int, optional
            The verbosity level.

        Notes
        -----
        When using a priority queue, the message to send to the queue has
        to be a tuple containing (priority, data) with priority 0 being the
        highest priority.
        '''

        super(ZmqSubRepQueue, self).__init__(queue_type=queue_type)
        self.verbosity = verbosity
        self._std_add_to_queue = False

        # fixme: this should use the connection selector.
        if input_mode == 'client':
            self.receiver = (
                ClientSub(
                    host_in,
                    port_in,
                    topic=topic
                )
            )
        elif input_mode == 'server':
            self.receiver = (
                ServerSub(
                    port=port_in,
                    topic=topic
                )
            )

        if output_mode == 'client':
            self.sender = (
                ClientRep(
                    host_out,
                    port_out
                )
            )
        elif output_mode == 'server':
            self.sender = (
                ServerRep(
                    port=port_out
                )
            )

        self.start()

    ##############
    # sub_thread #
    ##############

    def _sub_thread(self):
        '''
        Subscription thread.
        '''

        if self.verbosity > 0:
            print('Starting sub thread')

        try:
            while not self._stopped():
                if self.receiver.check_incoming(timeout=0.1):
                    message_in = self.receiver.receive_message()

                    if self.verbosity > 1:
                        print('Received: {}'.format(message_in))

                    message_in = self.process_incoming_data(message_in)

            if self.verbosity > 0:
                print('subscription thread stopped')

        except Exception:
            raise

    ###################
    # _req_rep_thread #
    ###################

    def _req_rep_thread(self):
        '''
        Request reply thread.
        '''

        if self.verbosity > 0:
            print('Starting request reply thread')

        try:
            while not self._stopped():
                if self.sender.check_incoming(timeout=0.1):
                    message_in = self.sender.receive_message()

                    if self.verbosity > 1:
                        print('Request received: {}'.format(message_in))

                    if message_in == 'next':
                        if self.queue_size() > 0:
                            message_out = self._queue.get()

                            # recompose message to remove priority if
                            # a user did not change the process_incoming_data
                            # function
                            if (
                                    self._queue_type == 'priority_queue'
                                    and self._std_add_to_queue):
                                message_out = (
                                    message_out[0][1],
                                    message_out[1]
                                )
                        else:
                            message_out = None

                        self.sender.send_message(message_out)

                    elif message_in == 'queue_size':
                        message_out = self.queue_size()
                        self.sender.send_message(message_out)
                    elif message_in == 'queue_type':
                        message_out = self.queue_type()
                        self.sender.send_message(message_out)
                    else:
                        self.sender.send_message(None)

            if self.verbosity > 0:
                print('Sender thread stopped')
        except Exception:
            raise

    ################
    # add_to_queue #
    ################

    def add_to_queue(self, data):
        '''
        Adds data to the queue.

        Parameters
        ----------
        data : any
            The data that needs to be added to the queue.
        '''

        self._queue.put(data)

    #########################
    # process_incoming_data #
    #########################

    def process_incoming_data(self, data):
        '''
        Class that allows manipulation of the incoming data.
        The method can be replaced by using the add_method_to_class decorator.

        Parameters
        ----------
        data : any

        Returns
        -------
        any
            The manipulated data.
        '''

        self._std_add_to_queue = True
        self.add_to_queue(data)

    #######
    # run #
    #######

    def run(self):
        sub_thread = threading.Thread(target=self._sub_thread)
        req_rep_thread = threading.Thread(target=self._req_rep_thread)

        sub_thread.start()
        req_rep_thread.start()

    ########
    # stop #
    ########

    def stop(self, linger=None):
        '''
        Method to stop the thread.

        Parameters
        ----------
        linger : float, optional
            The time in seconds to linger before stopping the connection.
        '''

        self._stop = True
        time.sleep(1)
        self.receiver.stop(linger=linger)
        self.sender.stop(linger=linger)


###################
# ZmqPullRepQueue #
###################

class ZmqPullRepQueue(_ZeroMQQueue):
    '''
    Class facilitating a ZMQ enabled Server Pull-Rep queue.
    '''

    ############
    # __init__ #
    ############

    def __init__(
            self,
            port_in=None,
            port_out=None,
            queue_type='fifo',
            verbosity=0):
        '''
        Class providing a multi config queue with ZMQ in and outputs.
        Input is a Pull socket while the output is a Reply socket.

        Parameters
        ----------
        port_in : int, optional
            The port on which the pull server runs.

        port_out : int, optional
            The port on which the reply server runs.

        queue_type : str, optional
            The type of queue to use.
            The following queues are available:
            + fifo
            + lifo
            + priority_queue

        verbosity : int, optional
            The verbosity level.
        '''

        self.verbosity = verbosity
        self.receiver = ServerPull(
            port=port_in
        )

        self.sender = ServerRep(
            port=port_out
        )

        super(ZmqPullRepQueue, self).__init__(queue_type=queue_type)

        self.start()

    ############
    # __exit__ #
    ############

    def __exit__(self, *args):
        if self.verbosity > 5:
            print('Exit called.')
        self.stop()

    ###############
    # pull_thread #
    ###############

    def pull_thread(self):

        if self.verbosity > 0:
            print('Starting pull thread')

        try:
            while not self._stopped():
                if (self.receiver.check_incoming(timeout=0.2)
                        and not self._stopped()):
                    _messageIn = self.receiver.receive_message()

                    if self.verbosity > 0:
                        print('Pulled: {}'.format(_messageIn))

                    self._queue.put(_messageIn)

        except Exception as err:
            print(err)

        finally:
            self.receiver.stop()
            if self.verbosity > 0:
                print('Receiver thread stopped')

    ##################
    # req_rep_thread #
    ##################

    def req_rep_thread(self):

        if self.verbosity > 0:
            print('Starting request reply thread')

        while not self._stopped():
            if self.sender.check_incoming(timeout=0.2):
                _messageIn = self.sender.receive_message()

                if self.verbosity > 0:
                    print('Request received: {}'.format(_messageIn))

                if _messageIn == 'next':
                    if self.queue_size() > 0:
                        _messageOut = self._queue.get()
                        self._queue.task_done()
                    else:
                        _messageOut = None

                    if (
                            isinstance(_messageOut, list)
                            or isinstance(_messageOut, tuple)):
                        _messageOut = _messageOut[1]

                    self.sender.send_message(_messageOut)

                elif _messageIn == 'queue_size':
                    _messageOut = self.queue_size()
                    self.sender.send_message(_messageOut)
                else:
                    self.sender.send_message(None)

        self.sender.stop()
        if self.verbosity > 0:
            print('Sender thread stopped')

    #######
    # run #
    #######

    def run(self):
        pull_thread = threading.Thread(target=self.pull_thread)
        req_rep_thread = threading.Thread(target=self.req_rep_thread)

        pull_thread.start()
        req_rep_thread.start()


##################
# ZmqRepRepProxy #
##################

class ZmqRepRepProxy(object):
    '''
    Class facilitating a ZMQ enabled Rep Rep Proxy.
    '''

    ############
    # __init__ #
    ############

    def __init__(
            self,
            frontend_port=None,
            backend_port=None,
            frontend_protocol=None,
            backend_protocol=None,
            verbosity=0):
        '''
        Class providing a multi config queue with ZMQ in and outputs.
        Both ports are Reply sockets.

        Parameters
        ----------
        frontend_port : int, optional
            The port on which the input runs.

        backend_port : int, optional
            The port on which the output runs.

        frontend_protocol : str, optional
            The protocol for the frontend.

        backend_protocol : str, optional
            The protocol for the backend,.

        verbosity : int, optional
            The verbosity level.

        Notes
        -----
        The client connected to the backend needs to have the proxy connection
        enabled.
        The client connected to the frontend does not need to enable the proxy
        connection.
        '''

        # super(ZmqRepRepProxy, self).__init__(verbosity=verbosity)
        self.verbosity = verbosity
        self.frontend = _ZeroMQProxy(verbosity=verbosity)
        self.frontend._init_router_socket(
            port=frontend_port,
            protocol=frontend_protocol
        )
        self.frontend_msg_queue = self.frontend._init_queue()

        self.backend = _ZeroMQProxy(verbosity=verbosity)
        self.backend._init_router_socket(
            port=backend_port,
            protocol=backend_protocol
        )

        self.backend_addr_queue = self.backend._init_queue()
        self.backend_msg_queue = self.backend._init_queue()

        self.run()

    #############################
    # _backend_receiving_thread #
    #############################

    def _backend_receiving_thread(self):
        if self.verbosity > 0:
            print('started backend receiving thread')

        while not self.backend._stopped():
            # Receive message from backend.
            if self.backend.check_incoming(timeout=0.1):
                msg = self.backend.receive_message()
                if self.verbosity > 0:
                    print('Backend rcv: {}'.format(msg))
                self.backend_addr_queue.put(msg[0])

                if msg[2] != self.backend._BE_READY:
                    self.backend_msg_queue.put(msg[2:])

        if self.verbosity > 0:
            print('stopped backend receiving thread')

    ###########################
    # _backend_sending_thread #
    ###########################

    def _backend_sending_thread(self):
        if self.verbosity > 0:
            print('started backend sending thread')

        while not self.backend._stopped():
            # Send message to backend.
            if (
                    self.backend_addr_queue.qsize() > 0
                    and self.frontend_msg_queue.qsize() > 0):
                fe_msg = self.frontend_msg_queue.get()
                be_addr = self.backend_addr_queue.get()
                self.backend.send_message(fe_msg, addr=be_addr)
                if self.verbosity > 0:
                    print('frontend msg sent to backend')
            else:
                time.sleep(1e-6)

        if self.verbosity > 0:
            print('stopped backend sending thread')

    ##############################
    # _frontend_receiving_thread #
    ##############################

    def _frontend_receiving_thread(self):
        if self.verbosity > 0:
            print('started frontend receiving thread')

        while not self.frontend._stopped():
            # Get message from fronted.
            if self.frontend.check_incoming(timeout=0.1):
                msg = self.frontend.receive_message()
                if self.verbosity > 0:
                    print('Frontend rcv: {}'.format(msg))
                self.frontend_msg_queue.put(msg)

        if self.verbosity > 0:
            print('stopped frontend receiving thread')

    ############################
    # _frontend_sending_thread #
    ############################

    def _frontend_sending_thread(self):
        if self.verbosity > 0:
            print('started frontend sending thread')

        while not self.frontend._stopped():
            # Send message to frontend.
            if self.backend_msg_queue.qsize() > 0:
                msg = self.backend_msg_queue.get()
                self.frontend.send_message(msg)
                if self.verbosity > 0:
                    print('backend msg sent to frontend')
            else:
                time.sleep(1e-6)

        if self.verbosity > 0:
            print('stopped frontend sending thread')

    ##########################
    # get_connection_details #
    ##########################

    def get_connection_details(self):
        '''
        Returns the current binding addresses of the front and backend.

        Returns
        -------
        tuple
            Tuple containing a list of frontend details and backend details.
            Each containing protocol, IP address, and port.
        '''
        fe = self.frontend.get_connection_details()
        be = self.backend.get_connection_details()

        return (fe, be)

    #######
    # run #
    #######

    def run(self):
        fe_receiving_thread = threading.Thread(
            target=self._frontend_receiving_thread
        )
        fe_sending_thread = threading.Thread(
            target=self._frontend_sending_thread
        )

        be_receiving_thread = threading.Thread(
            target=self._backend_receiving_thread
        )
        be_sending_thread = threading.Thread(
            target=self._backend_sending_thread
        )

        fe_receiving_thread.start()
        fe_sending_thread.start()
        be_receiving_thread.start()
        be_sending_thread.start()

    ########
    # stop #
    ########

    def stop(self):
        self.frontend.stop()
        self.backend.stop()
