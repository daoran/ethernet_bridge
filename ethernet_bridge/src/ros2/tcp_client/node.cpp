// _GNU_SOURCE for MSG_NOSIGNAL / MSG_DONTWAIT.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "node.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <vector>

#include <ethernet_msgs/msg/event_type.hpp>

using namespace std::chrono_literals;

namespace
{
constexpr std::size_t kRecvBuf = 65536;
}

TcpClient::TcpClient(const rclcpp::NodeOptions & options)
: rclcpp::Node("ethernet_bridge_tcp_client", options)
{
  cfg_.topic_busToHost = declare_parameter<std::string>("topic_busToHost", "bus_to_host");
  cfg_.topic_hostToBus = declare_parameter<std::string>("topic_hostToBus", "host_to_bus");
  cfg_.topic_event = declare_parameter<std::string>("topic_event", "event");
  cfg_.frame = declare_parameter<std::string>("frame", "");
  cfg_.ethernet_peerAddress = declare_parameter<std::string>("ethernet_peerAddress", "127.0.0.1");
  cfg_.ethernet_peerPort = declare_parameter<int>("ethernet_peerPort", 55555);
  cfg_.ethernet_bufferSize = declare_parameter<int>("ethernet_bufferSize", 0);
  cfg_.ethernet_reconnectInterval = declare_parameter<int>("ethernet_reconnectInterval", 500);

  pub_busToHost_ = create_publisher<ethernet_msgs::msg::Packet>(
    cfg_.topic_busToHost, rclcpp::QoS(rclcpp::KeepLast(100)));
  pub_event_ = create_publisher<ethernet_msgs::msg::Event>(
    cfg_.topic_event, rclcpp::QoS(rclcpp::KeepLast(1)).transient_local());
  sub_hostToBus_ = create_subscription<ethernet_msgs::msg::Packet>(
    cfg_.topic_hostToBus, rclcpp::QoS(rclcpp::KeepLast(100)),
    std::bind(&TcpClient::onHostToBus, this, std::placeholders::_1));

  publishEvent(ethernet_msgs::msg::EventType::DISCONNECTED);

  RCLCPP_INFO(
    get_logger(), "connecting to %s:%d ...", cfg_.ethernet_peerAddress.c_str(),
    cfg_.ethernet_peerPort);

  running_ = true;
  conn_thread_ = std::thread(&TcpClient::connectionLoop, this);
}

TcpClient::~TcpClient()
{
  running_ = false;
  {
    std::lock_guard<std::mutex> lk(sock_mutex_);
    if (fd_ >= 0) ::shutdown(fd_, SHUT_RDWR);  // unblock poll()/recv()
  }
  if (conn_thread_.joinable()) conn_thread_.join();
}

bool TcpClient::tryConnect()
{
  int s = ::socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0) return false;

  if (cfg_.ethernet_bufferSize > 0) {
    const int b = cfg_.ethernet_bufferSize;
    ::setsockopt(s, SOL_SOCKET, SO_RCVBUF, &b, sizeof(b));
  }

  sockaddr_in peer{};
  peer.sin_family = AF_INET;
  peer.sin_port = htons(static_cast<uint16_t>(cfg_.ethernet_peerPort));
  if (::inet_pton(AF_INET, cfg_.ethernet_peerAddress.c_str(), &peer.sin_addr) != 1) {
    RCLCPP_ERROR(get_logger(), "invalid ethernet_peerAddress '%s'", cfg_.ethernet_peerAddress.c_str());
    ::close(s);
    return false;
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

  // cache endpoints (network-order bytes == dotted-quad order)
  sockaddr_in la{};
  sockaddr_in pa{};
  socklen_t ll = sizeof(la);
  socklen_t pl = sizeof(pa);
  ::getsockname(s, reinterpret_cast<sockaddr *>(&la), &ll);
  ::getpeername(s, reinterpret_cast<sockaddr *>(&pa), &pl);
  std::memcpy(local_ip_.data(), &la.sin_addr.s_addr, 4);
  std::memcpy(peer_ip_.data(), &pa.sin_addr.s_addr, 4);
  local_port_ = ntohs(la.sin_port);
  peer_port_ = ntohs(pa.sin_port);

  {
    std::lock_guard<std::mutex> lk(sock_mutex_);
    fd_ = s;
    connected_ = true;
  }
  RCLCPP_INFO(get_logger(), "connected to %s:%d", cfg_.ethernet_peerAddress.c_str(), cfg_.ethernet_peerPort);
  return true;
}

void TcpClient::dropConnection(bool emit_event)
{
  int oldfd = -1;
  {
    std::lock_guard<std::mutex> lk(sock_mutex_);
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

void TcpClient::connectionLoop()
{
  std::vector<uint8_t> buf(kRecvBuf);

  while (running_.load(std::memory_order_relaxed)) {
    bool conn;
    {
      std::lock_guard<std::mutex> lk(sock_mutex_);
      conn = connected_;
    }

    if (!conn) {
      if (!tryConnect()) {
        // back off reconnectInterval (in small steps so shutdown stays responsive)
        const int step = 50;
        int slept = 0;
        const int target = cfg_.ethernet_reconnectInterval > 0 ? cfg_.ethernet_reconnectInterval : 500;
        while (running_.load(std::memory_order_relaxed) && slept < target) {
          std::this_thread::sleep_for(std::chrono::milliseconds(step));
          slept += step;
        }
        if (cfg_.ethernet_reconnectInterval <= 0) break;  // auto-reconnect disabled
        continue;
      }
      publishEvent(ethernet_msgs::msg::EventType::CONNECTED);
      RCLCPP_INFO(get_logger(), "connected.");
    }

    int fd;
    {
      std::lock_guard<std::mutex> lk(sock_mutex_);
      fd = fd_;
    }
    if (fd < 0) continue;

    pollfd pfd{fd, POLLIN, 0};
    const int pr = ::poll(&pfd, 1, 500);
    if (!running_.load(std::memory_order_relaxed)) break;
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

void TcpClient::onHostToBus(ethernet_msgs::msg::Packet::ConstSharedPtr msg)
{
  std::lock_guard<std::mutex> lk(sock_mutex_);
  if (!connected_ || fd_ < 0) return;  // not connected -> drop (peer absent)
  const ssize_t sent = ::send(
    fd_, msg->payload.data(), msg->payload.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
  if (sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
    // broken pipe etc.: the conn thread will notice and reconnect
    RCLCPP_WARN(get_logger(), "send() failed: %s", std::strerror(errno));
  }
}

void TcpClient::publishEvent(uint8_t type, int32_t value)
{
  ethernet_msgs::msg::Event ev;
  ev.header.stamp = now();
  ev.header.frame_id = cfg_.frame;
  ev.type = type;
  ev.value = value;
  pub_event_->publish(ev);
}
