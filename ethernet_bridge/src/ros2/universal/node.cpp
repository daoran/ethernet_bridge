// _GNU_SOURCE for MSG_NOSIGNAL / MSG_DONTWAIT.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "node.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <vector>

#include <ethernet_msgs/msg/event_type.hpp>
#include <ethernet_msgs/utils.h>

using namespace std::chrono_literals;

namespace
{
constexpr std::size_t kRecvBuf = 65536;

// "u.u.u.u" pretty-printer for a network-order sockaddr_in address.
std::string ip4str(uint32_t netorder_addr)
{
  char s[INET_ADDRSTRLEN]{};
  ::inet_ntop(AF_INET, &netorder_addr, s, sizeof(s));
  return std::string(s);
}
}  // namespace

UniversalBridge::UniversalBridge(const rclcpp::NodeOptions & options)
: rclcpp::Node("ethernet_bridge_universal", options)
{
  cfg_.topic_busToHost = declare_parameter<std::string>("topic_busToHost", "bus_to_host");
  cfg_.topic_hostToBus = declare_parameter<std::string>("topic_hostToBus", "host_to_bus");
  cfg_.topic_event = declare_parameter<std::string>("topic_event", "event");
  cfg_.frame = declare_parameter<std::string>("frame", "");

  const std::string proto = declare_parameter<std::string>("ethernet_protocol", "tcp");
  if (proto == "udp" || proto == "UDP") {
    cfg_.ethernet_protocol = Protocol::Udp;
  } else if (proto == "tcp" || proto == "TCP") {
    cfg_.ethernet_protocol = Protocol::Tcp;
  } else {
    RCLCPP_WARN(get_logger(), "unknown protocol '%s', falling back to tcp", proto.c_str());
    cfg_.ethernet_protocol = Protocol::Tcp;
  }

  cfg_.ethernet_peerAddress = declare_parameter<std::string>("ethernet_peerAddress", "127.0.0.1");
  cfg_.ethernet_peerPort = declare_parameter<int>("ethernet_peerPort", 55555);
  cfg_.ethernet_bindAddress = declare_parameter<std::string>("ethernet_bindAddress", "0.0.0.0");
  cfg_.ethernet_bindPort = declare_parameter<int>("ethernet_bindPort", 0);
  cfg_.ethernet_bufferSize = declare_parameter<int>("ethernet_bufferSize", 0);
  cfg_.ethernet_reconnectInterval = declare_parameter<int>("ethernet_reconnectInterval", 500);
  cfg_.ethernet_dnsFollow = declare_parameter<bool>("ethernet_dnsFollow", false);
  cfg_.ethernet_dnsRefreshInterval = declare_parameter<int>("ethernet_dnsRefreshInterval", 5000);

  pub_busToHost_ = create_publisher<ethernet_msgs::msg::Packet>(
    cfg_.topic_busToHost, rclcpp::QoS(rclcpp::KeepLast(100)));
  pub_event_ = create_publisher<ethernet_msgs::msg::Event>(
    cfg_.topic_event, rclcpp::QoS(rclcpp::KeepLast(1)).transient_local());
  sub_hostToBus_ = create_subscription<ethernet_msgs::msg::Packet>(
    cfg_.topic_hostToBus, rclcpp::QoS(rclcpp::KeepLast(100)),
    std::bind(&UniversalBridge::onHostToBus, this, std::placeholders::_1));

  publishEvent(ethernet_msgs::msg::EventType::DISCONNECTED);

  RCLCPP_INFO(
    get_logger(), "%s bridge to %s:%d (ethernet_dnsFollow=%s)",
    cfg_.ethernet_protocol == Protocol::Tcp ? "TCP" : "UDP", cfg_.ethernet_peerAddress.c_str(),
    cfg_.ethernet_peerPort, cfg_.ethernet_dnsFollow ? "true" : "false");

  running_ = true;
  worker_thread_ = std::thread(&UniversalBridge::workerLoop, this);
}

UniversalBridge::~UniversalBridge()
{
  running_ = false;
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    if (fd_ >= 0) ::shutdown(fd_, SHUT_RDWR);  // unblock a parked poll()/recv()
  }
  if (worker_thread_.joinable()) worker_thread_.join();
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }  // UDP leaves the fd open; TCP already closed it
  }
}

bool UniversalBridge::resolvePeer(sockaddr_in & out)
{
  addrinfo hints{};
  hints.ai_family = AF_INET;  // IPv4 to match the ethernet_msgs 4-byte address fields
  hints.ai_socktype = cfg_.ethernet_protocol == Protocol::Tcp ? SOCK_STREAM : SOCK_DGRAM;

  addrinfo * res = nullptr;
  const std::string port = std::to_string(cfg_.ethernet_peerPort);
  const int rc = ::getaddrinfo(cfg_.ethernet_peerAddress.c_str(), port.c_str(), &hints, &res);
  if (rc != 0 || res == nullptr) {
    if (res) ::freeaddrinfo(res);
    return false;
  }
  std::memcpy(&out, res->ai_addr, sizeof(sockaddr_in));  // first A record
  ::freeaddrinfo(res);
  return true;
}

void UniversalBridge::pace(int ms)
{
  for (int slept = 0; running_.load(std::memory_order_relaxed) && slept < ms; slept += 50) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

void UniversalBridge::workerLoop()
{
  if (cfg_.ethernet_protocol == Protocol::Tcp) {
    runTcp();
  } else {
    runUdp();
  }
}

// ------------------------------- TCP -------------------------------

bool UniversalBridge::tcpConnect()
{
  sockaddr_in peer{};
  if (!resolvePeer(peer)) {
    RCLCPP_WARN_THROTTLE(
      get_logger(), *get_clock(), 30000, "could not resolve '%s'",
      cfg_.ethernet_peerAddress.c_str());
    return false;
  }

  int s = ::socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0) return false;

  if (cfg_.ethernet_bufferSize > 0) {
    const int b = cfg_.ethernet_bufferSize;
    ::setsockopt(s, SOL_SOCKET, SO_RCVBUF, &b, sizeof(b));
  }

  // Non-blocking connect, bounded by reconnectInterval (so an unreachable peer
  // doesn't stall on the OS connect timeout).
  const int flags = ::fcntl(s, F_GETFL, 0);
  ::fcntl(s, F_SETFL, flags | O_NONBLOCK);
  int r = ::connect(s, reinterpret_cast<sockaddr *>(&peer), sizeof(peer));
  if (r != 0) {
    if (errno != EINPROGRESS) { ::close(s); return false; }
    pollfd pfd{s, POLLOUT, 0};
    const int to = cfg_.ethernet_reconnectInterval > 0 ? cfg_.ethernet_reconnectInterval : 1000;
    const int pr = ::poll(&pfd, 1, to);
    if (pr <= 0) { ::close(s); return false; }
    int soerr = 0;
    socklen_t l = sizeof(soerr);
    ::getsockopt(s, SOL_SOCKET, SO_ERROR, &soerr, &l);
    if (soerr != 0) { ::close(s); return false; }
  }
  ::fcntl(s, F_SETFL, flags);  // back to blocking

  sockaddr_in la{};
  sockaddr_in pa{};
  socklen_t ll = sizeof(la);
  socklen_t pl = sizeof(pa);
  ::getsockname(s, reinterpret_cast<sockaddr *>(&la), &ll);
  ::getpeername(s, reinterpret_cast<sockaddr *>(&pa), &pl);

  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    fd_ = s;
    connected_ = true;
    peer_addr_ = pa;
    peer_valid_ = true;
    std::memcpy(local_ip_.data(), &la.sin_addr.s_addr, 4);
    std::memcpy(peer_ip_.data(), &pa.sin_addr.s_addr, 4);
    local_port_ = ntohs(la.sin_port);
    peer_port_ = ntohs(pa.sin_port);
  }
  RCLCPP_INFO(
    get_logger(), "connected to %s:%d (%s)", cfg_.ethernet_peerAddress.c_str(),
    cfg_.ethernet_peerPort, ip4str(pa.sin_addr.s_addr).c_str());
  return true;
}

void UniversalBridge::runTcp()
{
  std::vector<uint8_t> buf(kRecvBuf);
  bool first_attempt = true;
  auto last_resolve = std::chrono::steady_clock::now();

  while (running_.load(std::memory_order_relaxed)) {
    bool conn;
    {
      std::lock_guard<std::mutex> lk(state_mutex_);
      conn = connected_;
    }

    if (!conn) {
      if (!first_attempt) {
        // reconnectInterval <= 0 disables auto-reconnect (parity with tcp_client).
        if (cfg_.ethernet_reconnectInterval <= 0) break;
        pace(cfg_.ethernet_reconnectInterval);
        if (!running_.load(std::memory_order_relaxed)) break;
      }
      first_attempt = false;
      if (!tcpConnect()) continue;
      last_resolve = std::chrono::steady_clock::now();
      publishEvent(ethernet_msgs::msg::EventType::CONNECTED);
    }

    int fd;
    {
      std::lock_guard<std::mutex> lk(state_mutex_);
      fd = fd_;
    }
    if (fd < 0) continue;

    pollfd pfd{fd, POLLIN, 0};
    const int pr = ::poll(&pfd, 1, 500);
    if (!running_.load(std::memory_order_relaxed)) break;

    // ethernet_dnsFollow: re-resolve on cadence and switch on change, even if the link
    // is still healthy (the old IP may keep working, but DNS now points away).
    if (cfg_.ethernet_dnsFollow) {
      const auto nowt = std::chrono::steady_clock::now();
      const auto age =
        std::chrono::duration_cast<std::chrono::milliseconds>(nowt - last_resolve).count();
      if (age >= cfg_.ethernet_dnsRefreshInterval) {
        last_resolve = nowt;
        sockaddr_in re{};
        if (resolvePeer(re)) {
          uint32_t cur;
          {
            std::lock_guard<std::mutex> lk(state_mutex_);
            cur = peer_addr_.sin_addr.s_addr;
          }
          if (re.sin_addr.s_addr != cur) {
            RCLCPP_INFO(
              get_logger(), "peer '%s' DNS changed %s -> %s, reconnecting",
              cfg_.ethernet_peerAddress.c_str(), ip4str(cur).c_str(),
              ip4str(re.sin_addr.s_addr).c_str());
            dropConnection(true);
            continue;
          }
        }
      }
    }

    if (pr < 0) {
      if (errno == EINTR) continue;
      dropConnection(true);
      continue;
    }
    if (pr == 0) continue;  // timeout, still connected
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
      dropConnection(true);
      continue;
    }
    if (pfd.revents & POLLIN) {
      const ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
      if (n > 0) {
        ethernet_msgs::msg::Packet pkt;
        pkt.header.stamp = now();
        pkt.header.frame_id = cfg_.frame;
        pkt.sender_ip = peer_ip_;
        pkt.sender_port = peer_port_;
        pkt.receiver_ip = local_ip_;
        pkt.receiver_port = local_port_;
        pkt.payload.assign(buf.begin(), buf.begin() + n);
        pub_busToHost_->publish(pkt);
      } else if (n == 0) {
        dropConnection(true);  // peer closed
      } else {
        if (errno == EINTR) continue;
        if (running_.load(std::memory_order_relaxed)) {
          publishEvent(ethernet_msgs::msg::EventType::SOCKETERROR, errno);
          dropConnection(true);
        }
      }
    }
  }

  dropConnection(false);  // final cleanup, no event on shutdown
}

void UniversalBridge::dropConnection(bool emit_event)
{
  int oldfd = -1;
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    if (fd_ >= 0 || connected_) {
      oldfd = fd_;
      fd_ = -1;
      connected_ = false;
    }
  }
  if (oldfd >= 0) {
    ::shutdown(oldfd, SHUT_RDWR);
    ::close(oldfd);
  }
  if (emit_event) {
    publishEvent(ethernet_msgs::msg::EventType::DISCONNECTED);
    RCLCPP_INFO(get_logger(), "disconnected");
  }
}

// ------------------------------- UDP -------------------------------

bool UniversalBridge::udpOpen()
{
  int s = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (s < 0) {
    RCLCPP_ERROR(get_logger(), "socket() failed: %s", std::strerror(errno));
    return false;
  }

  const int one = 1;
  ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  if (cfg_.ethernet_bufferSize > 0) {
    const int b = cfg_.ethernet_bufferSize;
    ::setsockopt(s, SOL_SOCKET, SO_RCVBUF, &b, sizeof(b));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(cfg_.ethernet_bindPort));
  if (::inet_pton(AF_INET, cfg_.ethernet_bindAddress.c_str(), &addr.sin_addr) != 1) {
    RCLCPP_ERROR(get_logger(), "invalid ethernet_bindAddress '%s'", cfg_.ethernet_bindAddress.c_str());
    ::close(s);
    return false;
  }
  if (::bind(s, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    RCLCPP_ERROR(
      get_logger(), "bind(%s:%d) failed: %s", cfg_.ethernet_bindAddress.c_str(),
      cfg_.ethernet_bindPort, std::strerror(errno));
    ::close(s);
    return false;
  }

  sockaddr_in la{};
  socklen_t ll = sizeof(la);
  ::getsockname(s, reinterpret_cast<sockaddr *>(&la), &ll);  // resolves an ephemeral bind port

  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    fd_ = s;
    std::memcpy(local_ip_.data(), &la.sin_addr.s_addr, 4);
    local_port_ = ntohs(la.sin_port);
  }
  RCLCPP_INFO(get_logger(), "bound to %s:%d", cfg_.ethernet_bindAddress.c_str(), ntohs(la.sin_port));
  return true;
}

void UniversalBridge::runUdp()
{
  if (!udpOpen()) {
    publishEvent(ethernet_msgs::msg::EventType::SOCKETERROR, -1);
    return;
  }

  std::vector<uint8_t> buf(kRecvBuf);
  int fd;
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    fd = fd_;
  }

  // Resolve the peer up front; keep retrying below if it isn't resolvable yet.
  auto last_resolve = std::chrono::steady_clock::now();
  {
    sockaddr_in re{};
    if (resolvePeer(re)) {
      std::lock_guard<std::mutex> lk(state_mutex_);
      peer_addr_ = re;
      peer_valid_ = true;
      std::memcpy(peer_ip_.data(), &re.sin_addr.s_addr, 4);
      peer_port_ = ntohs(re.sin_port);
      RCLCPP_INFO(
        get_logger(), "peer '%s' -> %s:%d", cfg_.ethernet_peerAddress.c_str(),
        ip4str(re.sin_addr.s_addr).c_str(), ntohs(re.sin_port));
    } else {
      RCLCPP_WARN(get_logger(), "could not resolve '%s' yet", cfg_.ethernet_peerAddress.c_str());
    }
  }

  // Poll timeout: short enough to service the DNS refresh / initial-resolve retry.
  const int poll_to = std::max(1, std::min(500, cfg_.ethernet_dnsFollow ? cfg_.ethernet_dnsRefreshInterval : 500));

  while (running_.load(std::memory_order_relaxed)) {
    pollfd pfd{fd, POLLIN, 0};
    const int pr = ::poll(&pfd, 1, poll_to);
    if (!running_.load(std::memory_order_relaxed)) break;

    // (re)resolve: on the follow_dns cadence, or keep trying until first valid.
    bool need_initial;
    {
      std::lock_guard<std::mutex> lk(state_mutex_);
      need_initial = !peer_valid_;
    }
    if (cfg_.ethernet_dnsFollow || need_initial) {
      const auto nowt = std::chrono::steady_clock::now();
      const auto age =
        std::chrono::duration_cast<std::chrono::milliseconds>(nowt - last_resolve).count();
      const int period = cfg_.ethernet_dnsFollow ? cfg_.ethernet_dnsRefreshInterval
                                         : std::max(1, cfg_.ethernet_reconnectInterval);
      if (age >= period) {
        last_resolve = nowt;
        sockaddr_in re{};
        if (resolvePeer(re)) {
          bool changed = false;
          {
            std::lock_guard<std::mutex> lk(state_mutex_);
            changed = !peer_valid_ || re.sin_addr.s_addr != peer_addr_.sin_addr.s_addr;
            if (changed) {
              peer_addr_ = re;
              peer_valid_ = true;
              std::memcpy(peer_ip_.data(), &re.sin_addr.s_addr, 4);
              peer_port_ = ntohs(re.sin_port);
            }
          }
          if (changed) {
            RCLCPP_INFO(
              get_logger(), "peer '%s' -> %s:%d", cfg_.ethernet_peerAddress.c_str(),
              ip4str(re.sin_addr.s_addr).c_str(), ntohs(re.sin_port));
          }
        }
      }
    }

    if (pr < 0) {
      if (errno == EINTR) continue;
      if (running_.load(std::memory_order_relaxed)) {
        RCLCPP_ERROR(get_logger(), "poll() failed: %s", std::strerror(errno));
      }
      continue;
    }
    if (pr == 0) continue;  // timeout -> loop for the DNS refresh
    if (!(pfd.revents & POLLIN)) continue;

    // Drain everything currently queued (non-blocking) before polling again.
    for (;;) {
      sockaddr_in src{};
      socklen_t sl = sizeof(src);
      const ssize_t n = ::recvfrom(
        fd, buf.data(), buf.size(), MSG_DONTWAIT, reinterpret_cast<sockaddr *>(&src), &sl);
      if (n < 0) {
        if (errno == EINTR) continue;
        break;  // EAGAIN/EWOULDBLOCK -> queue drained
      }
      ethernet_msgs::msg::Packet pkt;
      pkt.header.stamp = now();
      pkt.header.frame_id = cfg_.frame;
      pkt.sender_ip = ethernet_msgs::arrayByNativeIp4(ntohl(src.sin_addr.s_addr));
      pkt.sender_port = ntohs(src.sin_port);
      {
        std::lock_guard<std::mutex> lk(state_mutex_);
        pkt.receiver_ip = local_ip_;
        pkt.receiver_port = local_port_;
      }
      pkt.payload.assign(buf.begin(), buf.begin() + n);
      pub_busToHost_->publish(pkt);
    }
  }
}

// ---------------------------- send path ----------------------------

void UniversalBridge::onHostToBus(ethernet_msgs::msg::Packet::ConstSharedPtr msg)
{
  if (cfg_.ethernet_protocol == Protocol::Tcp) {
    std::lock_guard<std::mutex> lk(state_mutex_);
    if (!connected_ || fd_ < 0) return;  // not connected -> drop (peer absent)
    const ssize_t sent = ::send(
      fd_, msg->payload.data(), msg->payload.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
    if (sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
      RCLCPP_WARN(get_logger(), "send() failed: %s", std::strerror(errno));
    }
    return;
  }

  // UDP: always send to the fixed, resolved peer; the per-packet receiver_ip/port
  // is ignored (that's what the udp node is for).
  sockaddr_in dst{};
  int fd;
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    if (!peer_valid_ || fd_ < 0) return;  // peer not resolved yet
    dst = peer_addr_;
    fd = fd_;
  }
  const ssize_t sent =
    ::sendto(fd, msg->payload.data(), msg->payload.size(), 0,
             reinterpret_cast<sockaddr *>(&dst), sizeof(dst));
  if (sent < 0) {
    RCLCPP_WARN(get_logger(), "sendto() failed: %s", std::strerror(errno));
  }
}

void UniversalBridge::publishEvent(uint8_t type, int32_t value)
{
  ethernet_msgs::msg::Event ev;
  ev.header.stamp = now();
  ev.header.frame_id = cfg_.frame;
  ev.type = type;
  ev.value = value;
  pub_event_->publish(ev);
}
