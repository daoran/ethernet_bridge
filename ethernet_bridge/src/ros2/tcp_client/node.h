#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include <rclcpp/rclcpp.hpp>

#include <ethernet_msgs/msg/event.hpp>
#include <ethernet_msgs/msg/packet.hpp>

/**
 * ROS 2 TCP client bridge (from scratch — no Qt, no Asio).
 *
 * A dedicated connection thread owns the TCP socket lifecycle: (re)connect to
 * the peer (bounded by reconnectInterval via a non-blocking connect), then
 * poll()+recv() the inbound stream and publish bus_to_host; on close/error it
 * emits a DISCONNECTED event and reconnects. The rclcpp executor runs the
 * host_to_bus subscription -> send().
 *
 * Unlike the UDP nodes the fd is replaced on each reconnect, so the fd and the
 * connected flag are guarded by a mutex: the executor's send() and the
 * connection thread's close/reopen are mutually exclusive. (recv on the conn
 * thread and send on the executor on a live TCP fd are kernel-safe.)
 */
class TcpClient : public rclcpp::Node
{
public:
  explicit TcpClient(const rclcpp::NodeOptions & options = rclcpp::NodeOptions());
  ~TcpClient() override;

private:
  struct Configuration
  {
    std::string topic_busToHost;
    std::string topic_hostToBus;
    std::string topic_event;
    std::string frame;
    std::string ethernet_peerAddress;
    int ethernet_peerPort{55555};
    int ethernet_bufferSize{0};
    int ethernet_reconnectInterval{500};  // ms; <=0 disables auto-reconnect
  } cfg_;

  rclcpp::Subscription<ethernet_msgs::msg::Packet>::SharedPtr sub_hostToBus_;
  rclcpp::Publisher<ethernet_msgs::msg::Packet>::SharedPtr pub_busToHost_;
  rclcpp::Publisher<ethernet_msgs::msg::Event>::SharedPtr pub_event_;

  std::thread conn_thread_;
  std::atomic<bool> running_{false};

  std::mutex sock_mutex_;  // guards fd_ + connected_
  int fd_{-1};
  bool connected_{false};

  // per-connection endpoints, set by the conn thread at connect, read by it when publishing
  std::array<uint8_t, 4> peer_ip_{};
  std::array<uint8_t, 4> local_ip_{};
  uint16_t peer_port_{0};
  uint16_t local_port_{0};

  void connectionLoop();   // conn thread
  bool tryConnect();       // conn thread
  void dropConnection(bool emit_event);  // conn thread (+ dtor only does shutdown)
  void onHostToBus(ethernet_msgs::msg::Packet::ConstSharedPtr msg);  // executor thread
  void publishEvent(uint8_t type, int32_t value = 0);
};
