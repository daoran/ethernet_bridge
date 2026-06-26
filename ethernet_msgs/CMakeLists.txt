cmake_minimum_required(VERSION 3.5)
project(ethernet_msgs)

if("$ENV{ROS_VERSION}" STREQUAL "1")
  # ========================= ROS 1 (catkin) =========================
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11" CACHE STRING "" FORCE)

  find_package(catkin REQUIRED COMPONENTS
    std_msgs
    message_generation
  )

  add_message_files(
    FILES
    Packet.msg
    GenericPacket.msg
    ProtocolType.msg
    Event.msg
    EventType.msg
    Packets.msg
  )

  generate_messages(
    DEPENDENCIES
    std_msgs
  )

  catkin_package(
    INCLUDE_DIRS include
    CATKIN_DEPENDS std_msgs
  )

  include_directories(
    ${catkin_INCLUDE_DIRS}
  )

elseif("$ENV{ROS_VERSION}" STREQUAL "2")
  # ===================== ROS 2 (ament / rosidl) =====================
  find_package(ament_cmake REQUIRED)
  find_package(rosidl_default_generators REQUIRED)
  find_package(std_msgs REQUIRED)
  find_package(builtin_interfaces REQUIRED)

  rosidl_generate_interfaces(${PROJECT_NAME}
    msg/Packet.msg
    msg/GenericPacket.msg
    msg/ProtocolType.msg
    msg/Event.msg
    msg/EventType.msg
    msg/Packets.msg
    DEPENDENCIES std_msgs builtin_interfaces
  )

  # hand-written helper header (single utils.h works for both ROS via __has_include).
  # Modern ament include layout: install under include/${PROJECT_NAME}/ and export
  # that dir, so #include <ethernet_msgs/utils.h> resolves alongside the generated
  # rosidl headers (which live under the same include/ethernet_msgs/ prefix).
  install(
    DIRECTORY include/
    DESTINATION include/${PROJECT_NAME}
  )
  ament_export_include_directories(include/${PROJECT_NAME})

  ament_package()

else()
  message(FATAL_ERROR
    "ethernet_msgs: ROS_VERSION not set or unsupported: '$ENV{ROS_VERSION}'")
endif()
