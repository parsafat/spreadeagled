#!/usr/bin/env python3

import asyncio
import socket
import uuid
import yaml
from websockets.server import serve

users = yaml.safe_load(open('users.yaml', 'r'))

def parse_message(message):
    protocol_version = message[0:1]
    user_id = uuid.UUID(bytes=message[1:17])

    port = int.from_bytes(message[19:21], byteorder='big')

    match message[21:22]:
        case b'\x01':
            address = socket.inet_ntop(socket.AF_INET, message[22:26])
            request_data = message[26:]
        case b'\x02':
            address_length = int.from_bytes(message[22:23], byteorder='big')
            address = message[23:23 + address_length].decode()
            request_data = message[23 + address_length:]
        case b'\x03':
            address = socket.inet_ntop(socket.AF_INET6, message[22:38])
            request_data = message[38:]

    return protocol_version, user_id, port, address, request_data

class DataBridgeProtocol(asyncio.Protocol):
    def __init__(self, message, on_con_lost, protocol_version, websocket):
        self.message = message
        self.on_con_lost = on_con_lost
        self.protocol_version = protocol_version
        self.websocket = websocket
        self.first_message = True

    def connection_made(self, transport):
        transport.write(self.message)
        print('Data sent')

    def data_received(self, data):
        async def forward_to_client(data):
            if self.first_message:
                data = self.protocol_version + b'\x00' + data
                self.first_message = False
            await self.websocket.send(data)

        loop = asyncio.get_event_loop()
        loop.create_task(forward_to_client(data))
        print('Data received')

    def connection_lost(self, exc):
        print('The server closed the connection')
        self.on_con_lost.set_result(True)

async def handle(websocket):
    message = await websocket.recv()
    protocol_version, user_id, port, address, request_data = \
        parse_message(message)

    if not str(user_id) in users:
        return

    loop = asyncio.get_running_loop()
    on_con_lost = loop.create_future()

    transport, protocol = await loop.create_connection(
        lambda: DataBridgeProtocol(
            request_data, on_con_lost, protocol_version, websocket),
        address, port)

    async def forward_to_server():
        async for message in websocket:
            transport.write(message)

    await asyncio.wait(
        [
            asyncio.create_task(forward_to_server()),
            on_con_lost
        ],
        return_when=asyncio.FIRST_COMPLETED
    )

async def main():
    async with serve(handle, 'localhost', 8765):
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())
