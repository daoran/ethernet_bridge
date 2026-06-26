#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include <rclcpp/rclcpp.hpp>

#include <ethernet_msgs/msg/event.hpp>
#include <ethernet_msgs/msg/packet.hpp>
#include <ethernet_msgs/msg/packets.hpp>

/**
 * ROS 2 UDP bundler bridge (from scratch — no Qt, no Asio).
 *
 * Like the udp node, but aggregates inbound datagrams into one Packets message
 * to cut ROS overhead. Flush triggers (matching ROS 1): packet count, maximum
 * packet age, maximum idle time.
 *
 * Threading: the rclcpp executor runs host_to_bus -> sendto. ONE dedicated
 * thread runs a poll()-driven loop: poll() waits on the socket with a timeout
 * equal to the next bundle deadline, so inbound data AND the age/idle timers
 * are serviced on that single thread (poll() times out reliably, unlike
 * recvmmsg()'s timeout). The bundle buffer is therefore touched only by the rx
 * thread during operation; the destructor flushes the remainder only AFTER
 * joining that thread, so no lock is required.
 */
class UdpBundler : public rclcpp::Node
{
public:
  explicit UdpBundler(const rclcpp::NodeOptions & options = rclcpp::NodeOptions());
  ~UdpBundler() override;

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
    int trigger_numberOfPackets{50};
    int trigger_maximumPacketAge{10};  // ms; <=0 disables
    int trigger_maximumIdleTime{2};    // ms; <=0 disables
  } cfg_;

  rclcpp::Subscription<ethernet_msgs::msg::Packet>::SharedPtr sub_hostToBus_;
  rclcpp::Publisher<ethernet_msgs::msg::Packets>::SharedPtr pub_busToHost_;
  rclcpp::Publisher<ethernet_msgs::msg::Event>::SharedPtr pub_event_;

  int fd_{-1};
  std::thread rx_thread_;
  std::atomic<bool> running_{false};

  // bundle buffer — rx-thread-only during operation (destructor flushes after join)
  ethernet_msgs::msg::Packets buffer_;

  bool openSocket();
  void receiveLoop();                                                // rx thread
  void transmitBuffer();                                             // rx thread (+ dtor after join)
  void onHostToBus(ethernet_msgs::msg::Packet::ConstSharedPtr msg);  // executor thread
  void publishEvent(uint8_t type, int32_t value = 0);
};
