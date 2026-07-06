// _GNU_SOURCE for recvmmsg()/struct mmsghdr/MSG_DONTWAIT/IP_PKTINFO.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "node.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <ethernet_msgs/msg/event_type.hpp>
#include <ethernet_msgs/utils.h>

namespace
{
constexpr int kBatch = 16;
constexpr std::size_t kDatagramMax = 65536;

#if !defined(__linux__)
// macOS/BSD lack recvmmsg(2)/struct mmsghdr/MSG_WAITFORONE (the batch-receive
// syscall is Linux-only). Emulate it with a loop of recvmsg(2): flags pass
// through to each recvmsg; MSG_WAITFORONE means "block for the first datagram,
// then drain the rest without blocking", so after the first receive we OR in
// MSG_DONTWAIT. Returns the count received (or -1 with errno preserved when the
// very first receive fails). This node polls the fd and passes MSG_DONTWAIT, so
// every recvmsg here is non-blocking.
#ifndef MSG_WAITFORONE
#define MSG_WAITFORONE 0x10000  // synthetic (matches the Linux value); stripped below
#endif
struct mmsghdr
{
  msghdr msg_hdr;
  unsigned int msg_len;
};

int recvmmsg(int fd, mmsghdr * msgvec, unsigned int vlen, int flags, const void * /*timeout*/)
{
  const bool waitforone = (flags & MSG_WAITFORONE) != 0;
  int per_msg_flags = flags & ~MSG_WAITFORONE;  // MSG_WAITFORONE is emulated, not a real recvmsg flag
  unsigned int received = 0;
  for (; received < vlen; ++received) {
    const ssize_t r = ::recvmsg(fd, &msgvec[received].msg_hdr, per_msg_flags);
    if (r < 0) {
      if (received == 0) return -1;  // propagate errno (EAGAIN/EWOULDBLOCK/real error)
      break;                         // already have datagrams -> report them
    }
    msgvec[received].msg_len = static_cast<unsigned int>(r);
    if (waitforone) per_msg_flags |= MSG_DONTWAIT;  // block only for the first
  }
  return static_cast<int>(received);
}
#endif  // !__linux__
}  // namespace

UdpBundler::UdpBundler(const rclcpp::NodeOptions & options)
: rclcpp::Node("ethernet_bridge_udp_bundler", options)
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
  cfg_.trigger_numberOfPackets = declare_parameter<int>("trigger_numberOfPackets", 50);
  cfg_.trigger_maximumPacketAge = declare_parameter<int>("trigger_maximumPacketAge", 10);
  cfg_.trigger_maximumIdleTime = declare_parameter<int>("trigger_maximumIdleTime", 2);

  pub_busToHost_ = create_publisher<ethernet_msgs::msg::Packets>(
    cfg_.topic_busToHost, rclcpp::QoS(rclcpp::KeepLast(100)));
  pub_event_ = create_publisher<ethernet_msgs::msg::Event>(
    cfg_.topic_event, rclcpp::QoS(rclcpp::KeepLast(1)).transient_local());
  sub_hostToBus_ = create_subscription<ethernet_msgs::msg::Packet>(
    cfg_.topic_hostToBus, rclcpp::QoS(rclcpp::KeepLast(100)),
    std::bind(&UdpBundler::onHostToBus, this, std::placeholders::_1));

  if (cfg_.trigger_numberOfPackets > 0) {
    buffer_.packets.reserve(static_cast<std::size_t>(cfg_.trigger_numberOfPackets));
  }

  publishEvent(ethernet_msgs::msg::EventType::DISCONNECTED);

  if (!openSocket()) {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    publishEvent(ethernet_msgs::msg::EventType::SOCKETERROR, -1);
    throw std::runtime_error("ethernet_bridge_udp_bundler: socket setup failed");
  }

  running_ = true;
  rx_thread_ = std::thread(&UdpBundler::receiveLoop, this);
}

UdpBundler::~UdpBundler()
{
  running_ = false;
  if (fd_ >= 0) {
    ::shutdown(fd_, SHUT_RDWR);  // unblock poll()
  }
  if (rx_thread_.joinable()) {
    rx_thread_.join();
  }
  // rx thread has exited -> flushing the remainder here is single-threaded
  // (mirrors the ROS 1 destructor flush).
  if (!buffer_.packets.empty()) {
    transmitBuffer();
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool UdpBundler::openSocket()
{
  fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd_ < 0) {
    RCLCPP_ERROR(get_logger(), "socket() failed: %s", std::strerror(errno));
    return false;
  }

  const int one = 1;
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

  RCLCPP_INFO(
    get_logger(), "bound to %s:%d (bundler: n=%d age=%dms idle=%dms)", cfg_.ethernet_bindAddress.c_str(),
    cfg_.ethernet_bindPort, cfg_.trigger_numberOfPackets, cfg_.trigger_maximumPacketAge,
    cfg_.trigger_maximumIdleTime);
  return true;
}

void UdpBundler::receiveLoop()
{
  using clock = std::chrono::steady_clock;

  std::vector<std::array<uint8_t, kDatagramMax>> bufs(kBatch);
  std::vector<sockaddr_in> srcs(kBatch);
  std::vector<std::array<uint8_t, CMSG_SPACE(sizeof(in_pktinfo))>> ctrls(kBatch);
  std::vector<iovec> iovs(kBatch);
  std::vector<mmsghdr> msgs(kBatch);

  bool age_active = false;
  bool idle_active = false;
  clock::time_point age_deadline;
  clock::time_point idle_deadline;

  while (running_.load(std::memory_order_relaxed)) {
    // poll() timeout = time until the next active bundle deadline (block if none)
    int timeout_ms = -1;
    if (!buffer_.packets.empty() && (age_active || idle_active)) {
      clock::time_point dl = age_active ? age_deadline : idle_deadline;
      if (age_active && idle_active) dl = std::min(age_deadline, idle_deadline);
      const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(dl - clock::now()).count();
      timeout_ms = ms < 0 ? 0 : static_cast<int>(ms);
    }

    pollfd pfd{};
    pfd.fd = fd_;
    pfd.events = POLLIN;
    const int pr = ::poll(&pfd, 1, timeout_ms);
    if (pr < 0) {
      if (!running_.load(std::memory_order_relaxed)) break;
      if (errno == EINTR) continue;
      RCLCPP_ERROR(get_logger(), "poll() failed: %s", std::strerror(errno));
      publishEvent(ethernet_msgs::msg::EventType::SOCKETERROR, errno);
      continue;
    }

    if (pr > 0 && (pfd.revents & POLLIN)) {
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

      // Unqualified: resolves to glibc's ::recvmmsg on Linux, to the recvmsg-loop
      // fallback in the anonymous namespace above on macOS/BSD.
      const int n = recvmmsg(fd_, msgs.data(), kBatch, MSG_DONTWAIT, nullptr);
      if (n < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          if (!running_.load(std::memory_order_relaxed)) break;
          RCLCPP_ERROR(get_logger(), "recvmmsg() failed: %s", std::strerror(errno));
          publishEvent(ethernet_msgs::msg::EventType::SOCKETERROR, errno);
        }
      } else {
        for (int i = 0; i < n; ++i) {
          msghdr & h = msgs[i].msg_hdr;
          const std::size_t len = msgs[i].msg_len;
          const bool was_empty = buffer_.packets.empty();

          buffer_.packets.emplace_back();
          ethernet_msgs::msg::Packet & pkt = buffer_.packets.back();
          pkt.header.stamp = now();
          pkt.header.frame_id = cfg_.frame;
          pkt.sender_ip = ethernet_msgs::arrayByNativeIp4(ntohl(srcs[i].sin_addr.s_addr));
          pkt.sender_port = ntohs(srcs[i].sin_port);

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

          // age timer starts when the first packet enters an empty buffer
          if (was_empty && cfg_.trigger_maximumPacketAge > 0) {
            age_deadline = clock::now() + std::chrono::milliseconds(cfg_.trigger_maximumPacketAge);
            age_active = true;
          }
        }
        // idle timer restarts after every receive
        if (n > 0 && cfg_.trigger_maximumIdleTime > 0) {
          idle_deadline = clock::now() + std::chrono::milliseconds(cfg_.trigger_maximumIdleTime);
          idle_active = true;
        }
      }
    }

    // flush evaluation (count / age / idle)
    if (!buffer_.packets.empty()) {
      const auto nowtp = clock::now();
      const bool count_hit = cfg_.trigger_numberOfPackets > 0 &&
        static_cast<int>(buffer_.packets.size()) >= cfg_.trigger_numberOfPackets;
      const bool age_hit = age_active && nowtp >= age_deadline;
      const bool idle_hit = idle_active && nowtp >= idle_deadline;
      if (count_hit || age_hit || idle_hit) {
        transmitBuffer();
        age_active = false;
        idle_active = false;
      }
    }
  }
}

void UdpBundler::transmitBuffer()
{
  if (buffer_.packets.empty()) return;
  pub_busToHost_->publish(buffer_);
  buffer_.packets.clear();
}

void UdpBundler::onHostToBus(ethernet_msgs::msg::Packet::ConstSharedPtr msg)
{
  sockaddr_in dst{};
  dst.sin_family = AF_INET;
  dst.sin_addr.s_addr = htonl(ethernet_msgs::nativeIp4ByArray(msg->receiver_ip));
  dst.sin_port = htons(msg->receiver_port);

  iovec iov{};
  iov.iov_base = const_cast<uint8_t *>(msg->payload.data());
  iov.iov_len = msg->payload.size();

  msghdr mh{};
  mh.msg_name = &dst;
  mh.msg_namelen = sizeof(dst);
  mh.msg_iov = &iov;
  mh.msg_iovlen = 1;

  // Source-address override (ROS 1 Qt setSender()): sender_ip != 0 -> send from that
  // local source IP via IP_PKTINFO; sender_ip == 0 -> the bound address is used.
  // Only local addresses are valid (no spoofing); the source port stays the bound port.
  std::array<uint8_t, CMSG_SPACE(sizeof(in_pktinfo))> control{};
  const uint32_t src = ethernet_msgs::nativeIp4ByArray(msg->sender_ip);
  if (src != 0) {
    mh.msg_control = control.data();
    mh.msg_controllen = sizeof(control);
    cmsghdr * cm = CMSG_FIRSTHDR(&mh);
    cm->cmsg_level = IPPROTO_IP;
    cm->cmsg_type = IP_PKTINFO;
    cm->cmsg_len = CMSG_LEN(sizeof(in_pktinfo));
    in_pktinfo info{};
    info.ipi_spec_dst.s_addr = htonl(src);
    std::memcpy(CMSG_DATA(cm), &info, sizeof(info));
  }

  const ssize_t sent = ::sendmsg(fd_, &mh, 0);
  if (sent < 0) {
    RCLCPP_ERROR(get_logger(), "sendmsg() failed: %s", std::strerror(errno));
    publishEvent(ethernet_msgs::msg::EventType::SOCKETERROR, errno);
  }
}

void UdpBundler::publishEvent(uint8_t type, int32_t value)
{
  ethernet_msgs::msg::Event ev;
  ev.header.stamp = now();
  ev.header.frame_id = cfg_.frame;
  ev.type = type;
  ev.value = value;
  pub_event_->publish(ev);
}
