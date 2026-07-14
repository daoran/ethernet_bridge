#include "node.h"

#include <arpa/inet.h>

#include <cstring>
#include <stdexcept>

namespace
{
bool parseIpv4(const std::string & s, std::array<uint8_t, 4> & out)
{
  if (s.empty()) return false;
  in_addr a{};
  if (::inet_pton(AF_INET, s.c_str(), &a) != 1) return false;
  std::memcpy(out.data(), &a.s_addr, 4);
  return true;
}
}  // namespace

FileSink::FileSink(const rclcpp::NodeOptions & options)
: rclcpp::Node("ethernet_bridge_file_sink", options)
{
  cfg_.topic_in = declare_parameter<std::string>("topic_in", "bus_to_host");
  cfg_.file_name = declare_parameter<std::string>("file_name", "");
  cfg_.packet_delimiter = declare_parameter<std::string>("packet_delimiter", "");
  cfg_.filter_address = declare_parameter<std::string>("filter_address", "");
  cfg_.filter_port = declare_parameter<int>("filter_port", 0);

  // file_name from a launch `command="date ..."` often carries a trailing newline
  if (!cfg_.file_name.empty() && cfg_.file_name.back() == '\n') {
    cfg_.file_name.pop_back();
    RCLCPP_INFO(get_logger(), "removed trailing newline from file_name");
  }

  file_.open(cfg_.file_name, std::ofstream::out | std::ofstream::binary);
  if (!file_.is_open() || !file_.good()) {
    RCLCPP_ERROR(get_logger(), "could not open file '%s' for writing", cfg_.file_name.c_str());
    throw std::invalid_argument("file_sink: could not open file for writing");
  }
  RCLCPP_INFO(get_logger(), "writing to %s", cfg_.file_name.c_str());

  filter_ip_valid_ = parseIpv4(cfg_.filter_address, filter_ip_);
  if (filter_ip_valid_) {
    RCLCPP_INFO(get_logger(), "only storing packets with sender IP %s", cfg_.filter_address.c_str());
  }
  if (cfg_.filter_port) {
    RCLCPP_INFO(get_logger(), "only storing packets with sender port %d", cfg_.filter_port);
  }

  // best_effort by default (max publisher compatibility); qos_reliableSubscription:=true for reliable
  rclcpp::QoS sub_qos(rclcpp::KeepLast(100));
  if (!declare_parameter<bool>("qos_reliableSubscription", false))
    sub_qos.best_effort();
  sub_ = create_subscription<ethernet_msgs::msg::Packet>(
    cfg_.topic_in, sub_qos,
    std::bind(&FileSink::onIn, this, std::placeholders::_1));
}

FileSink::~FileSink()
{
  if (file_.is_open()) file_.close();
}

void FileSink::onIn(ethernet_msgs::msg::Packet::ConstSharedPtr msg)
{
  if (cfg_.filter_port && msg->sender_port != cfg_.filter_port) return;
  if (filter_ip_valid_ && msg->sender_ip != filter_ip_) return;

  file_.write(
    reinterpret_cast<const char *>(msg->payload.data()),
    static_cast<std::streamsize>(msg->payload.size()));
  if (!cfg_.packet_delimiter.empty()) file_ << cfg_.packet_delimiter;
  file_.flush();
}
