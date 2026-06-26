#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include <rclcpp/rclcpp.hpp>

#include <ethernet_msgs/msg/event.hpp>
#include <ethernet_msgs/msg/packet.hpp>

/**
 * ROS 2 UDP bridge (from scratch — no Qt, no Asio).
 *
 * ROS 2 has no librosqt single-loop equivalent (DDS does not expose a pollable
 * fd that could be folded into one event loop). Threading model instead:
 *   - the rclcpp executor thread runs the host_to_bus subscription callback,
 *     transmitting via a blocking sendto();
 *   - ONE dedicated thread blocks in recvmmsg() for inbound UDP and publishes
 *     bus_to_host.
 *
 * Shared state between the two threads is only: the UDP socket (concurrent
 * recv+send on a single UDP socket is kernel-safe — independent rx/tx paths)
 * and the rclcpp publishers (publish() is thread-safe). There is no other
 * mutable shared state, so this (non-bundling) node needs no lock.
 */
class UdpBridge : public rclcpp::Node
{
public:
  explicit UdpBridge(const rclcpp::NodeOptions & options = rclcpp::NodeOptions());
  ~UdpBridge() override;

private:
  struct Configuration
  {
    std::string topic_busToHost;
    std::string topic_hostToBus;
    std::string topic_event;
    std::string frame;
    std::string ethernet_bindAddress;
    std::string ethernet_multicastGroup;
    int ethernet_bindPort{55555};
    int ethernet_receiveBufferSize{20 * 1024 * 1024};
  } cfg_;

  rclcpp::Subscription<ethernet_msgs::msg::Packet>::SharedPtr sub_hostToBus_;
  rclcpp::Publisher<ethernet_msgs::msg::Packet>::SharedPtr pub_busToHost_;
  rclcpp::Publisher<ethernet_msgs::msg::Event>::SharedPtr pub_event_;

  int fd_{-1};
  std::thread rx_thread_;
  std::atomic<bool> running_{false};

  bool openSocket();
  void receiveLoop();                                                  // rx thread
  void onHostToBus(ethernet_msgs::msg::Packet::ConstSharedPtr msg);    // executor thread
  void publishEvent(uint8_t type, int32_t value = 0);
};
