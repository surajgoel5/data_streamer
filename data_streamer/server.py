import time
import json
import numpy as np
import zmq
from datetime import datetime
import pickle

class Server:
    def __init__(self, 
                 ctrl_obj,                     # control object with callable methods
                 data_getter_name: str = "get_data",  # method name in ctrl_obj used for streaming
                 ip_address= "0.0.0.0",
                 ctrl_port: int = 42069, 
                 data_port: int = 42096,
                 ctrl_rcv_timeout: int = 1000,  # ms
                 target_publish_rate_hz: float = 200):
        
        self.ctrl_obj = ctrl_obj
        self.data_getter_name = data_getter_name
        self.ctrl_port = ctrl_port
        self.data_port = data_port
        self.ip_address=ip_address
        self.ctrl_addr = f"tcp://{ip_address}:{self.ctrl_port}"
        self.data_addr = f"tcp://{ip_address}:{self.data_port}"
        self.ctrl_rcv_timeout = ctrl_rcv_timeout
        self.target_publish_rate_hz = target_publish_rate_hz
        
        self.ctx = None
        self.ctrl = None
        self.pub = None
        self.running = False
        self.sockets_initiated = False
        
        self.init_sockets()
    
    # -------------------------------------------------------------------------
    def init_sockets(self):
        """Initialize ZeroMQ PUB and REP sockets."""
        self.ctx = zmq.Context.instance()
        
        # Control channel (REQ/REP)
        self.ctrl = self.ctx.socket(zmq.REP)
        self.ctrl.bind(self.ctrl_addr)
        self.ctrl.RCVTIMEO = self.ctrl_rcv_timeout
        
        # Data channel (PUB/SUB)
        self.pub = self.ctx.socket(zmq.PUB)
        self.pub.bind(self.data_addr)
        
        # Tunables
        self.pub.setsockopt(zmq.SNDHWM, 100)
        self.pub.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.pub.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 60)
        
        self.sockets_initiated = True
        print(f"[Server] Listening on CTRL={self.ctrl_addr}, DATA={self.data_addr}")

    # -------------------------------------------------------------------------
    def stop_streaming(self):
        """Cleanly close sockets."""
        print("[Server] Stopping streaming and closing sockets.")
        if self.pub:
            self.pub.close()
        if self.ctrl:
            self.ctrl.close()
        if self.ctx:
            self.ctx.term()
        self.sockets_initiated = False
        self.running = False
    
    # -------------------------------------------------------------------------
    def _safe_serialize(self, obj):
        """Convert potentially non-JSON objects (e.g., numpy arrays) into safe formats."""
        if obj is None:
            return None
        try:
            json.dumps(obj)  # check if serializable
            return obj
        except (TypeError, OverflowError):
            pass

        # Handle common types
        if isinstance(obj, np.ndarray):
            return {
                "dtype": str(obj.dtype),
                "shape": obj.shape,
                "preview": obj.flatten()[:10].tolist()  # small preview only
            }
        elif isinstance(obj, (set, tuple)):
            return list(obj)
        elif isinstance(obj, (bytes, bytearray)):
            return f"<{len(obj)} bytes>"
        else:
            return str(obj)

    # -------------------------------------------------------------------------
    def ctrl_update(self, msg):
        """
        Handle incoming control messages.
        Expected message format:
          {
            "func": "method_name",
            "args": [arg1, arg2, ...],
            "kwargs": {"key": val, ...}
          }
        """
        func_name = msg.get("func")
        args = msg.get("args", [])
        kwargs = msg.get("kwargs", {})

        if not func_name:
            self.ctrl.send_json({"status": "error", "error": "Missing 'func' field"})
            return

        # Prevent calling private/protected or magic methods
        if func_name.startswith("_"):
            self.ctrl.send_json({"status": "error", "error": f"Access denied for method '{func_name}'"})
            return

        if not hasattr(self.ctrl_obj, func_name):
            self.ctrl.send_json({"status": "error", "error": f"No such method '{func_name}'"})
            return

        try:
            func = getattr(self.ctrl_obj, func_name)
            if not callable(func):
                raise TypeError(f"'{func_name}' is not callable")

            result = func(*args, **kwargs)
            safe_result = self._safe_serialize(result)
            self.ctrl.send_json({"status": "ok", "result": safe_result})

        except Exception as e:
            self.ctrl.send_json({"status": "error", "error": str(e)})

    # -------------------------------------------------------------------------
    def start_streaming(self):
        """Main streaming loop."""
        if not self.sockets_initiated:
            self.init_sockets()

        seq = 0
        self.running = True
        print("[Server] Starting streaming loop.")
        
        try:
            while self.running:
                t0 = time.time()
                # Handle control requests (non-blocking)
                try:
                    msg = self.ctrl.recv_json(flags=zmq.NOBLOCK)
                    if msg is not None:
                        self.ctrl_update(msg)
                except zmq.Again:
                    pass  # no control message available
                
                # Get data from the control object
                try:
                    getter = getattr(self.ctrl_obj, self.data_getter_name)
                    data = getter()
                except Exception as e:
                    print(f"[Server] Data getter error: {e}")
                    time.sleep(0.5)
                    continue

                # Publish header + binary data
                if isinstance(data, tuple):
                    hdr = {
                        "seq": seq,
                        "timestamp": time.time(),
                        "dtype": 'pickle',
                        "shape": 'lmao nope',
                    }
                    self.pub.send_json(hdr, flags=zmq.SNDMORE)
                    self.pub.send(pickle.dumps(data))
                    seq += 1  
                elif isinstance(data, np.ndarray):
                    hdr = {
                        "seq": seq,
                        "timestamp": time.time(),
                        "dtype": str(data.dtype),
                        "shape": data.shape,
                    }
                    self.pub.send_json(hdr, flags=zmq.SNDMORE)
                    self.pub.send(data.tobytes())
                    seq += 1
                else:
                    print(f"[Server] Warning: data_getter returned non-numpy type: {type(data)}")
                t1 = time.time()
                # Control publish rate
                if self.target_publish_rate_hz < 1.0/(t1-t0):
                    time.sleep( (1.0 / self.target_publish_rate_hz) - 1.0/(t1-t0))

        except KeyboardInterrupt:
            print("[Server] Interrupted by user.")
        finally:
            self.stop_streaming()
