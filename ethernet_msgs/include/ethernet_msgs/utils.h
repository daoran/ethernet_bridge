#pragma once

// One helper header for BOTH ROS 1 and ROS 2.
// The generated Packet message lives at a different path/namespace per ROS
// version; select it at preprocess time. __has_include needs no build-system
// cooperation and defaults safely to the ROS 1 layout when unavailable, so
// existing ROS 1 consumers (and their compilers) are unaffected.
#if defined(__has_include) && __has_include(<ethernet_msgs/msg/packet.hpp>)
  #include <ethernet_msgs/msg/packet.hpp>            // ROS 2 (rosidl)
  namespace ethernet_msgs { namespace detail { using PacketMsg = ::ethernet_msgs::msg::Packet; } }
#else
  #include <ethernet_msgs/Packet.h>                  // ROS 1 (catkin)
  namespace ethernet_msgs { namespace detail { using PacketMsg = ::ethernet_msgs::Packet; } }
#endif

namespace ethernet_msgs
{
static inline uint32_t nativeIp4ByArray(detail::PacketMsg::_sender_ip_type const& array)
{
    return 256*256*256 * static_cast<uint8_t>(array.at(0)) + 256*256 * static_cast<uint8_t>(array.at(1)) + 256 * static_cast<uint8_t>(array.at(2)) + static_cast<uint8_t>(array.at(3));
}

static inline detail::PacketMsg::_sender_ip_type arrayByNativeIp4(uint32_t const& number)
{
    return detail::PacketMsg::_sender_ip_type{static_cast<uint8_t>(number >> 24), static_cast<uint8_t>(number >> 16), static_cast<uint8_t>(number >> 8), static_cast<uint8_t>(number)};
}

}
