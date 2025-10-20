data_streamer
=============

.. image:: https://img.shields.io/pypi/v/data_streamer.svg
    :target: https://pypi.python.org/pypi/data_streamer
    :alt: Latest PyPI version

A lightweight Python library for fast, continuous data streaming between a server and one or more clients over TCP/IP using ZeroMQ.

`data_streamer` allows you to publish large numerical arrays or structured data from a server, and receive them in real time on clients. It supports a dedicated control channel for remote function calls and a high-speed data channel for continuous streaming.

Installation
------------

To install, run this command in your terminal:

.. code-block:: bash

   pip install git+https://github.com/surajgoel5/data_streamer.git

From sources
------------

You can either clone the public repository:

.. code-block:: bash

   git clone git://github.com/surajgoel5/data_streamer

Or download the tarball:

.. code-block:: bash

   curl -OJL https://github.com/surajgoel5/data_streamer/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: bash

   python setup.py install

Requirements
^^^^^^^^^^^^

- pyzmq  
- numpy  

Usage
-----

### Server Example

.. code-block:: python

    import numpy as np
    from data_streamer import Server

    class MyController:
        def __init__(self):
            self.multiplier = 1.0

        def get_data(self):
            return np.arange(1000, dtype=np.float32) * self.multiplier

        def set_multiplier(self, val):
            self.multiplier = float(val)
            return {"status": "ok", "multiplier": self.multiplier}

        def ping(self):
            return "pong"

    if __name__ == "__main__":
        ctrl = MyController()
        srv = Server(ctrl_obj=ctrl, data_getter_name="get_data", target_publish_rate_hz=100)
        srv.start_streaming()

This example creates a simple controller that continuously streams numerical data arrays and supports remote commands over the control channel.

---

### Client Example

.. code-block:: python

    import time
    from data_streamer import Client

    def on_data(arr, hdr):
        print(f"Received frame {hdr['seq']} with shape {arr.shape}")

    client = Client(recv_callback=on_data)

    # Example control calls
    print(client.call("ping"))
    print(client.call("set_multiplier", args=[2.0]))

    # Retrieve buffered data
    time.sleep(1)
    hdr, arr = client.get_latest()
    print("Latest data:", hdr["seq"], arr[:5])

---

Custom Decompression
--------------------

The client can optionally define a decompression function to decode custom data formats or compressed byte streams:

.. code-block:: python

    import zlib
    import numpy as np

    def decompress_zlib(raw, hdr):
        if hdr.get("compressed"):
            raw = zlib.decompress(raw)
        dtype = np.dtype(hdr["dtype"])
        shape = tuple(hdr["shape"])
        return np.frombuffer(raw, dtype=dtype).reshape(shape)

    client = Client(decompress_func=decompress_zlib)

---

Compatibility
-------------

Tested on Python 3.8+.

---

Licence
-------

This project is licensed under the MIT License.

---

Authors
-------

`data_streamer` was written by Suraj Goel _  
with design and implementation assistance from OpenAI’s ChatGPT (GPT-5 model).
