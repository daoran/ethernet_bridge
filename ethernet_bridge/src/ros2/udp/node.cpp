// _GNU_SOURCE for recvmmsg()/struct mmsghdr/MSG_WAITFORONE/IP_PKTINFO.
// g++ defines it by default; be explicit so clang/other front-ends agree.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "node.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <ethernet_msgs/msg/event_type.hpp>
#include <ethernet_msgs/utils.h>

namespace
{
constexpr int kBatch = 16;                  // datagrams drained per recvmmsg() wakeup
constexpr std::size_t kDatagramMax = 65536; // max UDP datagram payload
}  // namespace

UdpBridge::UdpBridge(const rclcpp::NodeOptions & options)
: rclcpp::Node("ethernet_bridge_udp", options)
{
  cfg_.topic_busToHost = declare_parameter<std::string>("topic_busToHost", "bus_to_host");
  cfg_.topic_hostToBus = declare_parameter<std::string>("topic_hostToBus", "host_to_bus");
  cfg_.topic_event = declare_parameter<std::string>("topic_event", "event");
  cfg_.frame = declare_parameter<std::string>("frame", "");
  cfg_.ethernet_bindAddress = declare_parameter<std::string>("ethernet_bindAddress", "0.0.0.0");
  cfg_.ethernet_multicastGroup = declare_parameter<std::string>("ethernet_multicastGroup", "");
  cfg_.ethernet_bindPort = declare_parameter<int>("ethernet_bindPort", 55555);
  cfg_.ethernet_receiveBufferSize =
    declare_parameter<int>("ethernet_receiveBufferSize", 20 * 1024 * 1024);

  // QoS faithful to the ROS 1 node: reliable, queue depth 100. The event topic
  // is latched (transient_local). Very high-rate deployments can remap to
  // best_effort via external QoS overrides.
  pub_busToHost_ = create_publisher<ethernet_msgs::msg::Packet>(
    cfg_.topic_busToHost, rclcpp::QoS(rclcpp::KeepLast(100)));
  pub_event_ = create_publisher<ethernet_msgs::msg::Event>(
    cfg_.topic_event, rclcpp::QoS(rclcpp::KeepLast(1)).transient_local());
  sub_hostToBus_ = create_subscription<ethernet_msgs::msg::Packet>(
    cfg_.topic_hostToBus, rclcpp::QoS(rclcpp::KeepLast(100)),
    std::bind(&UdpBridge::onHostToBus, this, std::placeholders::_1));

  // UDP has no real connection state; mirror the ROS 1 node and announce the
  // (latched) disconnected baseline.
  publishEvent(ethernet_msgs::msg::EventType::DISCONNECTED);

  if (!openSocket()) {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    publishEvent(ethernet_msgs::msg::EventType::SOCKETERROR, -1);
    throw std::runtime_error("ethernet_bridge_udp: socket setup failed");
  }

  running_ = true;
  rx_thread_ = std::thread(&UdpBridge::receiveLoop, this);
}

UdpBridge::~UdpBridge()
{
  running_ = false;
  if (fd_ >= 0) {
    ::shutdown(fd_, SHUT_RDWR);  // unblock a recvmmsg() that is parked on the fd
  }
  if (rx_thread_.joinable()) {
    rx_thread_.join();
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool UdpBridge::openSocket()
{
  fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd_ < 0) {
    RCLCPP_ERROR(get_logger(), "socket() failed: %s", std::strerror(errno));
    return false;
  }

  const int one = 1;
  // ROS 1 used ShareAddress | ReuseAddressHint -> SO_REUSEADDR.
  ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  // Qt's QUdpSocket enables SO_BROADCAST by default; a raw POSIX socket does not,
  // so broadcast / directed-broadcast sends would otherwise fail with EACCES.
  ::setsockopt(fd_, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one));

  if (cfg_.ethernet_receiveBufferSize >= 0) {
    const int want = cfg_.ethernet_receiveBufferSize;
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &want, sizeof(want));
    int got = 0;
    socklen_t len = sizeof(got);
    ::getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &got, &len);
    RCLCPP_INFO(
      get_logger(), "SO_RCVBUF requested=%d, assigned=%d (kernel may double the value)", want, got);
    if (got < want) {
      RCLCPP_WARN(
        get_logger(),
        "SO_RCVBUF capped by kernel (got %d < requested %d) - raise net.core.rmem_max to avoid loss",
        got, want);
    }
  }

  // Capture the datagram destination address (Qt destinationAddress()).
  ::setsockopt(fd_, IPPROTO_IP, IP_PKTINFO, &one, sizeof(one));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(cfg_.ethernet_bindPort));
  if (::inet_pton(AF_INET, cfg_.ethernet_bindAddress.c_str(), &addr.sin_addr) != 1) {
    RCLCPP_ERROR(get_logger(), "invalid ethernet_bindAddress '%s'", cfg_.ethernet_bindAddress.c_str());
    return false;
  }
  if (::bind(fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    RCLCPP_ERROR(
      get_logger(), "bind(%s:%d) failed: %s", cfg_.ethernet_bindAddress.c_str(),
      cfg_.ethernet_bindPort, std::strerror(errno));
    return false;
  }

  if (!cfg_.ethernet_multicastGroup.empty()) {
    ip_mreq mreq{};
    if (::inet_pton(AF_INET, cfg_.ethernet_multicastGroup.c_str(), &mreq.imr_multiaddr) != 1) {
      RCLCPP_WARN(
        get_logger(), "invalid ethernet_multicastGroup '%s'", cfg_.ethernet_multicastGroup.c_str());
    } else {
      mreq.imr_interface.s_addr = htonl(INADDR_ANY);
      const bool ok = ::setsockopt(fd_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) == 0;
      RCLCPP_INFO(
        get_logger(), "joining multicast group %s -> %s", cfg_.ethernet_multicastGroup.c_str(),
        ok ? "ok" : "failed");
    }
  }

  RCLCPP_INFO(get_logger(), "bound to %s:%d", cfg_.ethernet_bindAddress.c_str(), cfg_.ethernet_bindPort);
  return true;
}

void UdpBridge::receiveLoop()
{
  // Per-slot storage for one recvmmsg() batch (heap, ~1 MB for kBatch=16).
  std::vector<std::array<uint8_t, kDatagramMax>> bufs(kBatch);
  std::vector<sockaddr_in> srcs(kBatch);
  std::vector<std::array<uint8_t, CMSG_SPACE(sizeof(in_pktinfo))>> ctrls(kBatch);
  std::vector<iovec> iovs(kBatch);
  std::vector<mmsghdr> msgs(kBatch);

  while (running_.load(std::memory_order_relaxed)) {
    for (int i = 0; i < kBatch; ++i) {
      iovs[i].iov_base = bufs[i].data();
      iovs[i].iov_len = bufs[i].size();
      msghdr & h = msgs[i].msg_hdr;
      h = msghdr{};
      h.msg_name = &srcs[i];
      h.msg_namelen = sizeof(sockaddr_in);
      h.msg_iov = &iovs[i];
      h.msg_iovlen = 1;
      h.msg_control = ctrls[i].data();
      h.msg_controllen = ctrls[i].size();
      msgs[i].msg_len = 0;
    }

    // MSG_WAITFORONE: block for the first datagram, then drain whatever else is
    // immediately available (up to kBatch) without further blocking.
    const int n = ::recvmmsg(fd_, msgs.data(), kBatch, MSG_WAITFORONE, nullptr);
    if (n < 0) {
      if (!running_.load(std::memory_order_relaxed)) break;  // shutdown()
      if (errno == EINTR) continue;
      RCLCPP_ERROR(get_logger(), "recvmmsg() failed: %s", std::strerror(errno));
      publishEvent(ethernet_msgs::msg::EventType::SOCKETERROR, errno);
      continue;
    }

    for (int i = 0; i < n; ++i) {
      msghdr & h = msgs[i].msg_hdr;
      const std::size_t len = msgs[i].msg_len;

      ethernet_msgs::msg::Packet pkt;
      pkt.header.stamp = now();
      pkt.header.frame_id = cfg_.frame;

      pkt.sender_ip = ethernet_msgs::arrayByNativeIp4(ntohl(srcs[i].sin_addr.s_addr));
      pkt.sender_port = ntohs(srcs[i].sin_port);

      // Destination address via IP_PKTINFO (== Qt destinationAddress()).
      uint32_t dst_native = 0;
      for (cmsghdr * cm = CMSG_FIRSTHDR(&h); cm != nullptr; cm = CMSG_NXTHDR(&h, cm)) {
        if (cm->cmsg_level == IPPROTO_IP && cm->cmsg_type == IP_PKTINFO) {
          in_pktinfo info{};
          std::memcpy(&info, CMSG_DATA(cm), sizeof(info));
          dst_native = ntohl(info.ipi_addr.s_addr);
          break;
        }
      }
      pkt.receiver_ip = ethernet_msgs::arrayByNativeIp4(dst_native);
      pkt.receiver_port = static_cast<uint16_t>(cfg_.ethernet_bindPort);

      pkt.payload.assign(bufs[i].begin(), bufs[i].begin() + len);

      pub_busToHost_->publish(pkt);
    }
  }
}

void UdpBridge::onHostToBus(ethernet_msgs::msg::Packet::ConstSharedPtr msg)
{
  sockaddr_in dst{};
  dst.sin_family = AF_INET;
  dst.sin_addr.s_addr = htonl(ethernet_msgs::nativeIp4ByArray(msg->receiver_ip));
  dst.sin_port = htons(msg->receiver_port);

  // NOTE: the ROS 1 node could override the *source* address (msg->sender_ip)
  // via Qt's setSender(); replicating that needs sendmsg()+IP_PKTINFO and is a
  // TODO. With sender_ip == 0 (the default) the bound address is used.
  const ssize_t sent = ::sendto(
    fd_, msg->payload.data(), msg->payload.size(), 0, reinterpret_cast<sockaddr *>(&dst),
    sizeof(dst));
  if (sent < 0) {
    RCLCPP_ERROR(get_logger(), "sendto() failed: %s", std::strerror(errno));
    publishEvent(ethernet_msgs::msg::EventType::SOCKETERROR, errno);
  }
}

void UdpBridge::publishEvent(uint8_t type, int32_t value)
{
  ethernet_msgs::msg::Event ev;
  ev.header.stamp = now();
  ev.header.frame_id = cfg_.frame;
  ev.type = type;
  ev.value = value;
  pub_event_->publish(ev);
}
