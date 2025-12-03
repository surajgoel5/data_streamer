import zmq
import json
import numpy as np
import threading
import time
from collections import deque
import pickle

class Client:
    def __init__(self,
                 ip_address: str = "localhost",
                 ctrl_port: int = 42069,
                 data_port: int = 42096,
                 subscribe: bool = True,
                 recv_callback=None,
                 decompress_func=None,
                 buffer_size: int = 10):
        """
        Parameters
        ----------
        ctrl_host, data_host : str
            Host/IP for control and data channels.
        ctrl_port, data_port : int
            Ports for control and data channels.
        subscribe : bool
            If True, start listening immediately.
        recv_callback : callable or None
            Optional function called when new data arrives.
                recv_callback(array, header_dict)
        decompress_func : callable or None
            Function to convert bytes->numpy array.
                decompress_func(raw_bytes, header_dict) -> np.ndarray
        buffer_size : int
            Number of most recent data frames to keep in memory.
        """
        self.ctrl_addr = f"tcp://{ip_address}:{ctrl_port}"
        self.data_addr = f"tcp://{ip_address}:{data_port}"

        self.ctx = zmq.Context.instance()
        self.req = self.ctx.socket(zmq.REQ)
        self.req.connect(self.ctrl_addr)
        self.req.RCVTIMEO = 5000

        self.sub = None
        self.recv_callback = recv_callback
        self.decompress_func = decompress_func
        self.running = False
        self.thread = None

        self.buffer = deque(maxlen=buffer_size)
        self.buffer_lock = threading.Lock()

        if subscribe:
            self._init_sub_socket()
            self.start_listening()

    # ----------------------------------------------------------------------
    def _init_sub_socket(self):
        """Initialize SUB socket."""
        self.sub = self.ctx.socket(zmq.SUB)
        self.sub.connect(self.data_addr)
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "")
        self.sub.setsockopt(zmq.RCVHWM, 100)
        print(f"[Client] Connected to CTRL={self.ctrl_addr}, DATA={self.data_addr}")

    # ----------------------------------------------------------------------
    def call(self, func: str, timeout: float = 5.0, *args, **kwargs):
        """Call a remote function on the server's ctrl_obj."""
        msg = {"func": func}
        if args:
            msg["args"] = args
        if kwargs:
            msg["kwargs"] = kwargs

        try:
            self.req.send_json(msg)
            self.req.RCVTIMEO = int(timeout * 1000)
            reply = self.req.recv_json()
            return reply
        except zmq.Again:
            return {"status": "error", "error": "Timeout waiting for reply"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # ----------------------------------------------------------------------
    def _decode_data(self, raw: bytes, hdr: dict):
        """Decode or decompress bytes into a NumPy array."""
        if self.decompress_func:
            return self.decompress_func(raw, hdr)
        # Default: direct reconstruction
        if hdr["dtype"]=='pickle':
            return pickle.loads(raw)
        else:
            dtype = np.dtype(hdr["dtype"])
            shape = tuple(hdr["shape"])
            return np.frombuffer(raw, dtype=dtype).reshape(shape)

    # ----------------------------------------------------------------------
    def _recv_loop(self):
        """Internal loop that continuously receives and decodes streamed data."""
        print("[Client] Data listening thread started.")
        while self.running:
            try:
                hdr = self.sub.recv_json(flags=zmq.NOBLOCK)
                raw = self.sub.recv(flags=0)
            except zmq.Again:
                time.sleep(0.001)
                continue
            except Exception as e:
                print(f"[Client] Receive error: {e}")
                break

            try:
                arr = self._decode_data(raw, hdr)
            except Exception as e:
                print(f"[Client] Decode error: {e}")
                continue

            # Save to buffer
            with self.buffer_lock:
                self.buffer.append((hdr, arr))

            # Optional callback
            if self.recv_callback:
                try:
                    self.recv_callback(arr, hdr)
                except Exception as cb_err:
                    print(f"[Client] Callback error: {cb_err}")

        print("[Client] Data listening thread stopped.")

    # ----------------------------------------------------------------------
    def get_latest(self):
        """Return the latest (header, array) tuple from buffer, or None."""
        with self.buffer_lock:
            if not self.buffer:
                return None
            return self.buffer[-1]

    # ----------------------------------------------------------------------
    def get_all(self):
        """Return a copy of all buffered (header, array) data."""
        with self.buffer_lock:
            return list(self.buffer)

    # ----------------------------------------------------------------------
    def start_listening(self):
        """Start background thread to receive published data."""
        if self.thread and self.thread.is_alive():
            return
        if self.sub is None:
            self._init_sub_socket()

        self.running = True
        self.thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.thread.start()

    # ----------------------------------------------------------------------
    def stop_listening(self):
        """Stop data receiving thread."""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)
        if self.sub:
            self.sub.close()
            self.sub = None

    # ----------------------------------------------------------------------
    def close(self):
        """Close all sockets and terminate context."""
        self.stop_listening()
        if self.req:
            self.req.close()
        self.ctx.term()
        print("[Client] Closed all connections.")
