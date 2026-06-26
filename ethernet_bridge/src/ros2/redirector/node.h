#pragma once

#include <array>
#include <cstdint>
#include <string>

#include <rclcpp/rclcpp.hpp>

#include <ethernet_msgs/msg/packet.hpp>

/**
 * ROS 2 packet redirector (from scratch). Pure ROS-to-ROS: no socket, no extra
 * thread — a single subscription callback on the executor rewrites the receiver
 * IP/port and/or filters by sender, then republishes. Functional drop-in for
 * the ROS 1 node.
 */
class Redirector : public rclcpp::Node
{
public:
  explicit Redirector(const rclcpp::NodeOptions & options = rclcpp::NodeOptions());

private:
  struct Configuration
  {
    std::string topic_in;
    std::string topic_out;
    std::string redirect_address;
    std::string filter_address;
    int redirect_port{0};
    int filter_port{0};
  } cfg_;

  rclcpp::Subscription<ethernet_msgs::msg::Packet>::SharedPtr sub_;
  rclcpp::Publisher<ethernet_msgs::msg::Packet>::SharedPtr pub_;

  std::array<uint8_t, 4> redirect_ip_{};
  bool redirect_ip_valid_{false};
  std::array<uint8_t, 4> filter_ip_{};
  bool filter_ip_valid_{false};

  void onIn(ethernet_msgs::msg::Packet::ConstSharedPtr msg);
};
