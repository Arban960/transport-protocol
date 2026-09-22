# Reliable Transport Protocol (RTP) over an Unreliable Network

A custom transport layer written in C, implementing a stop-and-wait reliable data transfer protocol on top of an unreliable, corruption-prone network simulation.

## What it does

A multithreaded client and Python server exchange ROT13-encrypted messages. The client encrypts and sends a message, the server decrypts it and sends it back, and the client prints the result. Underneath, the message travels across a network that randomly corrupts packets, so the transport layer has to detect corruption and recover from it without the application layer ever knowing.

## Protocol implemented

- **Packetization**: arbitrary-length messages are split into fixed-size packets, with the final packet flagged as `LAST_DATA` so the receiver knows when a message is complete
- **Checksums**: a custom weighted checksum (adjacent-byte swap, then sum of ASCII values times index) run on every packet to detect corruption
- **Stop-and-wait ARQ**: one packet in flight at a time, with the sender blocking until it receives an ACK or NACK
- **ACK/NACK handling**: corrupted packets trigger a NACK and retransmission of the last packet; valid packets are ACKed and the sender advances

## Key implementation details

- Separate sender and receiver threads per connection, coordinated through send/receive queues and condition variables
- Thread-safe signaling between the receiver thread (which reads ACK/NACK packets) and the sender thread (which is blocked waiting on them)
- Message reassembly on the receiver side, buffering packets until a `LAST_DATA` packet completes the message
- Full memory and concurrency safety: no leaks, all shared state protected by mutexes
- Unit-testable in isolation via `./rtp-client -t packetize` and `./rtp-client -t checksum`

## Files

- `rtp.c`: transport layer implementation (packetize, checksum, sender/receiver threads)
- `rtp.h`: connection struct and protocol definitions
- `network.c` / `network.h`: simulated unreliable network layer (provided)
- `client.c`: application layer, sends and receives ROT13 messages (provided)
- `rtp-server.py`: server that decrypts messages and echoes them back, with a configurable packet corruption rate

## Running it

```bash
make
python3 rtp-server.py -p 8080 -c 0.8
./rtp-client 127.0.0.1 8080
```

`-c` sets the network's corruption rate (0 to 1) so the retransmission logic can be exercised under different loss conditions.
