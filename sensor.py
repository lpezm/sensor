import socket
import selectors
import types
import numpy as np
import pandas as pd

sel = selectors.DefaultSelector()
HOST = '10.35.70.15'
PORT = 33000
i=0
#messages = [b"Hello, This is client 1 , How you doing.", b"That's all from me, cheers."]
df = pd.read_csv("foo2.csv")
messages = list(df.iloc[:, 1])


def start_connections(host, port, num_conns):
    server_addr = (host, port)
    for i in range(0, num_conns):
        connid = i + 1
        print("starting connection", connid, "to", server_addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.connect_ex(server_addr)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(
            connid=connid,
            msg_total=len(messages),
            recv_total=0,
            messages=list(messages),
            outb=b"",
        )
        sel.register(sock, events, data=data)


def service_connection(key, mask):
    i=0
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            print("received", repr(recv_data), "from connection", data.connid)
            data.recv_total += len(recv_data)
        if not recv_data or data.recv_total == data.msg_total:
            print("closing connection", data.connid)
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        while (True):
                if i > len(messages):
                    i=0
                data.outb= messages[i]
                print("sending", messages[i], "to connection", data.connid)
                sent = sock.send(data.outb)  # Should be ready to write
                i+=1
                sleep(0.1)

start_connections(HOST, PORT, 2)

try:
    while True:
        events = sel.select(timeout=1)
        if events:
            for key, mask in events:
                service_connection(key, mask)
        # Check for a socket being monitored to continue.
        if not sel.get_map():
            break
except KeyboardInterrupt:
    print("caught keyboard interrupt, exiting")
finally:
    sel.close()