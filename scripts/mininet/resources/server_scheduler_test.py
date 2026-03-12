#!/usr/bin/env python3
# Uploaded via server_scheduler_test.sh

from mininet.net import Mininet, Host
from mininet.log import setLogLevel
import os
import subprocess
from subprocess import Popen
from utils import HostParams, NetParams, createMininet
from mininet.util import pmonitor
import select

print("[INFO] Running processes on server and client...")


SERVER_MODE = os.environ['SERVER_MODE']
ABR_MODE = os.environ.get('ABR_MODE', 'bola')
SERVER_BW = float(os.environ['SERVER_BW'])
CLIENT_BW = float(os.environ['CLIENT_BW'])
LOSS = float(os.environ['LOSS'])
PARALELLISM = int(os.environ['PARALELLISM'])
DELAY = '%fms' % float(os.environ['DELAY'])
LOAD = float(os.environ['LOAD'])
BASE_LATENCY = int(os.environ['BASE_LATENCY'])

print('SERVER_MODE=', SERVER_MODE)
print('ABR_MODE=', ABR_MODE)
print('SERVER_BW=', SERVER_BW)
print('CLIENT_BW=', CLIENT_BW)
print('LOSS=', LOSS)
print('PARALELLISM=', PARALELLISM)
print('DELAY=', DELAY)
print('LOAD=', LOAD)
print('BASE_LATENCY=', BASE_LATENCY)

class Test():
    def __init__(self):
        net, server, clients = createMininet(NetParams(
            server=HostParams(bw=SERVER_BW, delay=DELAY, loss=LOSS),
            clients=[HostParams(bw=CLIENT_BW)]))
        self.server_policy: str = SERVER_MODE
        self.net: Mininet = net
        self.server: Host = server
        self.client: Host = clients[0]
        self.processes: 'dict[Host, Popen]' = {}
        self.iperf_server: 'Popen | None' = None
        self.iperf_client: 'Popen | None' = None

    def run(self):
        try:
            self.__run()
        finally:
            self.__finalize()

    def __run(self):
        print('Starting Mininet')
        self.net.start()
        print("Hosts in the network:", [host.name for host in self.net.hosts])
        print("Server IP:", self.server.IP())
        print("Client IP:", self.client.IP())
        
        if not self.net.waitConnected():
            raise RuntimeError('Failed to connect switches')
        setLogLevel('info')

        if LOAD != 0.0:
            load = '%dM' % (SERVER_BW * LOAD / 100)
            print('Starting load: ' + load)
            self.iperf_server = self.server.popen(
                ['iperf', '-u', '-s', '-p', '5001'])
            self.iperf_client = self.client.popen(
                ['iperf', '-u', '-c', self.server.IP(), '-p', '5001',
                 '-b', load, '-t', '99999'])

        print('Running test')
        dir = os.path.dirname(os.path.realpath(__file__))

        # Start server
        self.processes[self.server] = self.server.popen(
            [dir + '/main', 'server', self.server_policy],
            cwd=dir,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE)
        # Start client
        self.processes[self.client] = self.client.popen(
            ['env', f'ABR_MODE={ABR_MODE}', dir + '/main', 'test-client', self.server.IP(), str(PARALELLISM),
             str(BASE_LATENCY)],
            cwd=dir,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE)
        
        alive = dict(self.processes)

        for host, line in pmonitor(alive, timeoutms=1000):
            if host and line:
                print(f"[{host.name}] {line.strip()}")

    def __finalize(self):
        setLogLevel('warning')
        for process in self.processes.values():
            process.terminate()
        if self.iperf_client is not None:
            self.iperf_client.terminate()
        if self.iperf_server is not None:
            self.iperf_server.terminate()
            print('iperf server results:')
            result, _ = self.iperf_server.communicate()
            try:
                print(result.decode('utf-8', errors='ignore'))
            except Exception as e:
                print(f"[decode error: {e}]")
        self.net.stop()

Test().run()
