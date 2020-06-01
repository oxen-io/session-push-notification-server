import zmq
import time

context = zmq.Context()
socket = context.socket(zmq.DEALER)
socket.setsockopt(zmq.CONNECT_TIMEOUT, 5000)
socket.setsockopt(zmq.HANDSHAKE_IVL, 5000)

remote = "tcp://public.loki.foundation:9999"
socket.connect(remote)
socket.send_multipart([b'sub.mempool', b'_txallsub', b'all'])
last_sub_time = time.time()

while True:
    got_msg = socket.poll(timeout=5000)
    if last_sub_time + 60 < time.time():
        socket.send_multipart([b'sub.mempool', b'_txallsub', b'all'])
        last_sub_time = time.time()

    if not got_msg:
        continue

    m = socket.recv_multipart()
    print(m)
    if len(m) == 3 and m[0] == b'REPLY' and m[1] in (b'_blocksub', b'_txallsub', b'_txblinksub') and m[2] in (b'OK', b'ALREADY'):
        if m[2] == b'ALREADY':
            continue
    elif len(m) == 3 and m[0] == b'notify.mempool':
        print("New TX: {} {}".format(m[1].hex(), m[2].hex()))
