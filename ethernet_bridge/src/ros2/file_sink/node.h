#pragma once

#include <array>
#include <cstdint>
#include <fstream>
#include <string>

#include <rclcpp/rclcpp.hpp>

#include <ethernet_msgs/msg/packet.hpp>

/**
 * ROS 2 file sink (from scratch). Pure ROS: no socket, no extra thread — a
 * single subscription callback on the executor writes each packet's payload to
 * a file (optional delimiter, optional sender filter). Functional drop-in for
 * the ROS 1 node.
 */
class FileSink : public rclcpp::Node
{
public:
  explicit FileSink(const rclcpp::NodeOptions & options = rclcpp::NodeOptions());
  ~FileSink() override;

private:
  struct Configuration
  {
    std::string topic_in;
    std::string file_name;
    std::string packet_delimiter;
    std::string filter_address;
    int filter_port{0};
  } cfg_;

  rclcpp::Subscription<ethernet_msgs::msg::Packet>::SharedPtr sub_;
  std::ofstream file_;

  std::array<uint8_t, 4> filter_ip_{};
  bool filter_ip_valid_{false};

  void onIn(ethernet_msgs::msg::Packet::ConstSharedPtr msg);
};
