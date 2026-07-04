# ethernet_bridge — ROS 2 implementation

From-scratch ROS 2 ports of the five original nodes plus a new `universal` node
(no Qt, no Asio): only rclcpp + ethernet_msgs + POSIX sockets.

ROS 2 has no librosqt single-loop equivalent (DDS exposes no pollable fd), so
the socket nodes use the rclcpp executor for the ROS side plus one dedicated
thread for the socket:

- **udp** — blocking `recvmmsg()` rx thread → publish `Packet`; executor
  subscription → `sendto`.
- **udp_bundler** — `poll(fd, next-deadline)` + `recvmmsg(MSG_DONTWAIT)` rx
  thread, aggregating into `Packets` with count / max-age / max-idle flush
  triggers; the bundle buffer is rx-thread-only (the destructor flushes the
  remainder after joining the thread) → no lock.
- **tcp_client** — a connection thread owns the socket lifecycle (non-blocking
  connect bounded by `reconnectInterval`, `poll`+`recv` → publish, reconnect on
  drop with DISCONNECTED/CONNECTED events); executor subscription → `send`. The
  fd is replaced on reconnect, so the fd + connected flag are guarded by a mutex.
- **universal** — convenience node for the "talk to one named peer" case: one
  transport, TCP *or* UDP (`ethernet_protocol`), the peer given as a hostname or
  IP (`getaddrinfo`), and with `ethernet_dnsFollow` a periodic re-resolve that
  switches to a changed address even while the old one still works (TCP:
  reconnect, UDP: re-target). One worker thread owns the socket; the send
  destination is always the fixed resolved peer (use `udp` for arbitrary
  per-packet destinations).
- **redirector** — pure ROS (no socket, no extra thread): an executor
  subscription rewrites the receiver IP/port and/or filters by sender, then
  republishes.
- **file_sink** — pure ROS (no socket, no extra thread): an executor
  subscription writes payloads (+ optional delimiter, optional sender filter)
  to a file.

The cross-thread shared state in the ported socket nodes (the UDP socket; the
tcp fd via mutex; the thread-safe rclcpp publishers) is verified race-free under
load with ThreadSanitizer.
