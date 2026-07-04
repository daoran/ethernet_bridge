#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include <netinet/in.h>

#include <rclcpp/rclcpp.hpp>

#include <ethernet_msgs/msg/event.hpp>
#include <ethernet_msgs/msg/packet.hpp>

/**
 * ROS 2 "universal" bridge (from scratch — no Qt, no Asio).
 *
 * A convenience endpoint for the common "talk to one named peer" case that the
 * lean tcp_client / udp nodes deliberately do NOT cover:
 *   - one transport, TCP *or* UDP, selected by `protocol` (never both at once);
 *   - the peer may be a hostname (getaddrinfo — e.g. `calibrationpi.local`),
 *     not just a dotted-quad IPv4 like the other nodes' inet_pton;
 *   - with `ethernet_dnsFollow` it periodically re-resolves the name and switches to a
 *     changed address even while the old one still works (TCP: reconnect,
 *     UDP: re-target the sends) — for devices roaming behind mDNS / DHCP.
 *
 * One worker thread owns the socket lifecycle: (re)resolve + connect (TCP) or
 * bind (UDP), then poll()+recv() the inbound side -> publish bus_to_host, and
 * drive the DNS refresh. The rclcpp executor runs the host_to_bus subscription
 * -> send to the fixed, resolved peer. The destination is always that peer, not
 * the per-packet receiver_ip/port (use the udp node for arbitrary destinations).
 *
 * fd_/connected_ (TCP is reconnected -> the fd is replaced) and peer_addr_
 * (follow_dns mutates it) are shared with the executor's send() and guarded by
 * state_mutex_. recv on the worker and send on the executor over one socket are
 * kernel-safe (independent rx/tx paths).
 */
class UniversalBridge : public rclcpp::Node
{
public:
  explicit UniversalBridge(const rclcpp::NodeOptions & options = rclcpp::NodeOptions());
  ~UniversalBridge() override;

private:
  enum class Protocol { Tcp, Udp };

  struct Configuration
  {
    std::string topic_busToHost;
    std::string topic_hostToBus;
    std::string topic_event;
    std::string frame;
    Protocol ethernet_protocol{Protocol::Tcp};
    std::string ethernet_peerAddress;              // hostname or IPv4 (getaddrinfo)
    int ethernet_peerPort{55555};
    std::string ethernet_bindAddress{"0.0.0.0"};   // UDP: local bind address
    int ethernet_bindPort{0};                       // UDP: local bind port (0 = ephemeral)
    int ethernet_bufferSize{0};                     // SO_RCVBUF; <=0 leaves the kernel default
    int ethernet_reconnectInterval{500};            // ms; <=0 disables TCP auto-reconnect
    bool ethernet_dnsFollow{false};
    int ethernet_dnsRefreshInterval{5000};          // ms; re-resolve cadence when dnsFollow
  } cfg_;

  rclcpp::Subscription<ethernet_msgs::msg::Packet>::SharedPtr sub_hostToBus_;
  rclcpp::Publisher<ethernet_msgs::msg::Packet>::SharedPtr pub_busToHost_;
  rclcpp::Publisher<ethernet_msgs::msg::Event>::SharedPtr pub_event_;

  std::thread worker_thread_;
  std::atomic<bool> running_{false};

  std::mutex state_mutex_;      // guards fd_, connected_, peer_addr_/peer_valid_, endpoints
  int fd_{-1};
  bool connected_{false};       // TCP: link is up
  sockaddr_in peer_addr_{};     // current resolved peer (the send target)
  bool peer_valid_{false};

  // endpoints for bus_to_host provenance
  std::array<uint8_t, 4> peer_ip_{};
  std::array<uint8_t, 4> local_ip_{};
  uint16_t peer_port_{0};
  uint16_t local_port_{0};

  bool resolvePeer(sockaddr_in & out);              // getaddrinfo -> first AF_INET result
  void pace(int ms);                                // shutdown-responsive sleep (50 ms steps)

  void workerLoop();                                // worker thread: dispatch by protocol
  void runTcp();                                    // worker thread
  void runUdp();                                    // worker thread
  bool tcpConnect();                                // worker thread: resolve + connect
  bool udpOpen();                                   // worker thread: bind local socket
  void dropConnection(bool emit_event);            // worker thread (+ dtor shutdown only)

  void onHostToBus(ethernet_msgs::msg::Packet::ConstSharedPtr msg);  // executor thread
  void publishEvent(uint8_t type, int32_t value = 0);
};
