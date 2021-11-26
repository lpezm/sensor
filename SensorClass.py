#!/usr/bin/env python3

import socket
import selectors
import types
import numpy as np
import pandas as pd
import io
import pickle
import time
import re
class Sensor:

    def __init__(self, sensorType):
        match(sensorType):
            case 'temp':
                self.port = 33000
                df = pd.read_csv("foo2.csv")
                msgs = df.iloc[:, 1]
                msgs = ['{:.2f}'.format(x) for x in msgs]
                msgs = [str(x) for x in msgs]
                self.messages = [x.encode('utf-8') for x in msgs]
            case 'speed':
                self.port = 33000
                df = pd.read_csv("foo2.csv")
                msgs = df.iloc[:, 1]
                msgs = ['{:.2f}'.format(x) for x in msgs]
                msgs = [str(x) for x in msgs]
                self.messages = [x.encode('utf-8') for x in msgs]
        self.host = '10.35.70.15'
        self.selector = selectors.DefaultSelector()
        self.finished = False

    def start_connections(self, num_conns):
        server_addr = (self.host, self.port)
        for i in range(0, num_conns):
            connid = i + 1
            print("starting connection", connid, "to", server_addr)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            sock.connect_ex(server_addr)
            events = selectors.EVENT_READ | selectors.EVENT_WRITE
            data = types.SimpleNamespace(
                connid=connid,
                msg_total=sum(len(m) for m in self.messages),
                recv_total=0,
                messages=list(self.messages),
                outb=b"",
            )
            self.selector.register(sock, events, data=data)

    def service_connection(self, key, mask, sensorType):
        sock = key.fileobj
        data = key.data
        sensi = sensorType +": "
        sens = sensi.encode('utf-8')
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
            if not data.outb and data.messages:
                data.outb = data.messages.pop(0)
            if data.outb:
                print("sending", repr(data.outb), "to connection", data.connid)
                sent = sock.send(sens+data.outb)  # Should be ready to write
                data.outb = data.outb[sent:]
        return self.finished

    def run(self, sensorType):
        while not self.finished:
            try:
                events = sel.select(timeout=None)
                if events:
                    for key, mask in events:
                        self.service_connection(key, mask, sensorType)
                # Check for a socket being monitored to continue.
                if not sel.get_map():
                    break
            except KeyboardInterrupt:
                print("caught keyboard interrupt, exiting")
            finally:
                self.selector.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sensorType', help='Type of sensor', required=True)
    args = parser.parse_args()
    sensorType = args.sensorType
    mySensor = Sensor(sensorType)
    mySensor.start_connections(2)
    mySensor.run(sensorType)


if __name__ == '__main__':
    main()