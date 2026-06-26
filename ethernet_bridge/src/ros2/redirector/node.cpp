#include "node.h"

#include <arpa/inet.h>

#include <cstring>

namespace
{
// Parse a dotted IPv4 string into the 4-byte array used by ethernet_msgs
// (network byte order == dotted-quad order). Returns false on empty/invalid.
bool parseIpv4(const std::string & s, std::array<uint8_t, 4> & out)
{
  if (s.empty()) return false;
  in_addr a{};
  if (::inet_pton(AF_INET, s.c_str(), &a) != 1) return false;
  std::memcpy(out.data(), &a.s_addr, 4);
  return true;
}
}  // namespace

Redirector::Redirector(const rclcpp::NodeOptions & options)
: rclcpp::Node("ethernet_bridge_redirector", options)
{
  cfg_.topic_in = declare_parameter<std::string>("topic_in", "bus_to_host");
  cfg_.topic_out = declare_parameter<std::string>("topic_out", "host_to_bus");
  cfg_.redirect_address = declare_parameter<std::string>("redirect_address", "");
  cfg_.redirect_port = declare_parameter<int>("redirect_port", 0);
  cfg_.filter_address = declare_parameter<std::string>("filter_address", "");
  cfg_.filter_port = declare_parameter<int>("filter_port", 0);

  redirect_ip_valid_ = parseIpv4(cfg_.redirect_address, redirect_ip_);
  filter_ip_valid_ = parseIpv4(cfg_.filter_address, filter_ip_);

  pub_ = create_publisher<ethernet_msgs::msg::Packet>(
    cfg_.topic_out, rclcpp::QoS(rclcpp::KeepLast(100)));
  sub_ = create_subscription<ethernet_msgs::msg::Packet>(
    cfg_.topic_in, rclcpp::QoS(rclcpp::KeepLast(100)),
    std::bind(&Redirector::onIn, this, std::placeholders::_1));

  if (redirect_ip_valid_) {
    RCLCPP_INFO(get_logger(), "redirecting receiver IP to %s", cfg_.redirect_address.c_str());
  }
  if (cfg_.redirect_port) {
    RCLCPP_INFO(get_logger(), "redirecting receiver port to %d", cfg_.redirect_port);
  }
  if (filter_ip_valid_) {
    RCLCPP_INFO(get_logger(), "only forwarding packets with sender IP %s", cfg_.filter_address.c_str());
  }
  if (cfg_.filter_port) {
    RCLCPP_INFO(get_logger(), "only forwarding packets with sender port %d", cfg_.filter_port);
  }
  if (!redirect_ip_valid_ && !cfg_.redirect_port && !filter_ip_valid_ && !cfg_.filter_port) {
    RCLCPP_WARN(get_logger(), "no redirection or filtering active");
  }
}

void Redirector::onIn(ethernet_msgs::msg::Packet::ConstSharedPtr msg)
{
  // filter on sender (drop before copying)
  if (cfg_.filter_port && msg->sender_port != cfg_.filter_port) return;
  if (filter_ip_valid_ && msg->sender_ip != filter_ip_) return;

  ethernet_msgs::msg::Packet out = *msg;  // copy to rewrite the receiver
  if (cfg_.redirect_port) out.receiver_port = static_cast<uint16_t>(cfg_.redirect_port);
  if (redirect_ip_valid_) out.receiver_ip = redirect_ip_;
  pub_->publish(out);
}
