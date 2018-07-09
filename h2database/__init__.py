from h2database import constants
import logging
import socket
import struct

apilevel = '2.0'
threadsafety = 1

def connect(*args, **kwargs):
    c = Connection(*args, **kwargs)
    c.connect()
    return c

class Error(Exception):
    pass

class DatabaseError(Error):

    def __init__(self, message, sql, state, error_code, stack_trace):
        super().__init__(message)
        self.sql = sql
        self.state = state
        self.error_code = error_code
        self.stack_trace = stack_trace

class InternalError(DatabaseError):
    pass

logger = logging.getLogger(__name__)

def hash_password(user, password):
    if not user and not password:
        return b''
    raise NotImplemented

class Connection(object):

    def __init__(self, host, port, db, **kwargs):
        self.host = host
        self.port = port
        self.db = db
        self.user = kwargs.pop('user', '').upper()
        password = kwargs.pop('password', '')
        self.file_password_hash = None # TODO
        self.user_password_hash = hash_password(self.user, password)
        self.props = kwargs
        self.sock = None
        self.transfer = None

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logger.debug("Connecting to %s:%d", self.host, self.port)
            self.sock.connect((self.host, self.port))
            logger.debug("Initializing transfer to %s:%d", self.host, self.port)
            self.transfer = Transfer(self.sock)
            self.transfer.write_int(constants.TCP_PROTOCOL_VERSION_MIN_SUPPORTED)
            self.transfer.write_int(constants.TCP_PROTOCOL_VERSION_MAX_SUPPORTED)
            self.transfer.write_string(self.db)
            self.transfer.write_string(None) # original URL
            self.transfer.write_string(self.user)
            self.transfer.write_bytes(self.user_password_hash)
            self.transfer.write_bytes(self.file_password_hash)
            self.transfer.write_int(len(self.props))
            for k, v in self.props.items():
                self.transfer.write_string(k)
                self.transfer.write_string(v)
            self.done()
            logger.info("Connected to %s on %s:%d", self.db, self.host, self.port)
        except:
            self.close()
            raise

    def close(self):
        if self.transfer is not None:
            self.transfer.close()
            self.transfer = None
        if self.sock is not None:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None

    def done(self):
        self.transfer.flush()
        status = self.transfer.read_int()
        if status == constants.STATUS_ERROR:
            sqlstate = self.transfer.read_string()
            message = self.transfer.read_string()
            sql = self.transfer.read_string()
            error_code = self.transfer.read_int()
            stack_trace = self.transfer.read_string()
            raise DatabaseError(message, sql, sqlstate, error_code, stack_trace)
        elif status == constants.STATUS_CLOSED:
            self.close()
        elif status == constants.STATUS_OK_STATE_CHANGED:
            pass
        elif status == constants.STATUS_OK:
            pass
        else:
            raise InternalError("unexpected status %d" % status)

    def __enter__(self):
        if self.sock is None:
            self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class Transfer(object):

    BUFFER_SIZE = 64 * 1024

    def __init__(self, sock):
        self.stream = sock.makefile('rwb', self.BUFFER_SIZE)

    def read_int(self):
        return struct.unpack('>i', self.stream.read(4))[0]

    def read_char(self):
        return chr(struct.unpack('>H', self.stream.read(2))[0])

    def read_string(self):
        n = self.read_int()
        if n == -1:
            return None
        return ''.join(self.read_char() for _ in range(n))

    def write_int(self, x):
        self.stream.write(struct.pack('>i', x))

    def write_bytes(self, data):
        if data is None:
            self.write_int(-1)
        else:
            self.write_int(len(data))
            self.stream.write(data)

    def write_string(self, s):
        if s is None:
            self.write_int(-1)
        else:
            self.write_int(len(s))
            for c in s:
                self.write_char(ord(c))

    def write_char(self, c):
        self.stream.write(struct.pack('>H', c))

    def flush(self):
        self.stream.flush()

    def close(self):
        self.flush()
        self.stream.close()
