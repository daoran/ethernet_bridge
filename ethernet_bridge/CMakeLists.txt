cmake_minimum_required(VERSION 3.5)
project(ethernet_bridge)

if("$ENV{ROS_VERSION}" STREQUAL "1")
  # ================= ROS 1 (catkin + Qt5 + librosqt) =================
  # Single mainloop (ROS callback queue folded into the Qt event loop via
  # librosqt) -> lowest-latency, event-driven. Unchanged from the original.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11" CACHE STRING "" FORCE)

  find_package(catkin REQUIRED COMPONENTS
    roscpp
    std_msgs
    ethernet_msgs
    librosqt
  )

  find_package(Qt5 5.9 COMPONENTS
      Core
      Network
      REQUIRED
  )
  set(CMAKE_AUTOMOC ON)

  catkin_package(
    CATKIN_DEPENDS std_msgs ethernet_msgs
  )

  include_directories(
    ${catkin_INCLUDE_DIRS}
  )

  ## UDP Bridge
  add_executable(udp src/ros1/udp/main.cpp src/ros1/udp/node.cpp)
  target_link_libraries(udp ${catkin_LIBRARIES} Qt5::Core Qt5::Network)

  ## TCP Client Bridge
  add_executable(tcp_client src/ros1/tcp_client/main.cpp src/ros1/tcp_client/node.cpp)
  target_link_libraries(tcp_client ${catkin_LIBRARIES} Qt5::Core Qt5::Network)

  ## UDP Bundler Bridge
  add_executable(udp_bundler src/ros1/udp_bundler/main.cpp src/ros1/udp_bundler/node.cpp)
  target_link_libraries(udp_bundler ${catkin_LIBRARIES} Qt5::Core Qt5::Network)

  ## Redirector
  add_executable(redirector src/ros1/redirector/main.cpp src/ros1/redirector/node.cpp)
  target_link_libraries(redirector ${catkin_LIBRARIES} Qt5::Core Qt5::Network)

  ## File Sink
  add_executable(file_sink src/ros1/file_sink/main.cpp src/ros1/file_sink/node.cpp)
  target_link_libraries(file_sink ${catkin_LIBRARIES} Qt5::Core Qt5::Network)

elseif("$ENV{ROS_VERSION}" STREQUAL "2")
  # ===================== ROS 2 (ament + rclcpp) =====================
  # No librosqt single-loop equivalent in ROS 2: the rclcpp executor runs the
  # host_to_bus subscription (-> sendto) while ONE dedicated thread blocks in
  # recvmmsg() for inbound UDP (-> publish). No Qt, no Asio; only rclcpp +
  # ethernet_msgs + POSIX sockets.
  if(NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 17)
  endif()

  find_package(ament_cmake REQUIRED)
  find_package(rclcpp REQUIRED)
  find_package(std_msgs REQUIRED)
  find_package(ethernet_msgs REQUIRED)
  find_package(Threads REQUIRED)

  ## UDP Bridge
  add_executable(udp src/ros2/udp/main.cpp src/ros2/udp/node.cpp)
  ament_target_dependencies(udp rclcpp std_msgs ethernet_msgs)
  target_link_libraries(udp Threads::Threads)

  ## UDP Bundler Bridge
  add_executable(udp_bundler src/ros2/udp_bundler/main.cpp src/ros2/udp_bundler/node.cpp)
  ament_target_dependencies(udp_bundler rclcpp std_msgs ethernet_msgs)
  target_link_libraries(udp_bundler Threads::Threads)

  ## Packet Redirecting node (pure ROS — no socket, no extra thread)
  add_executable(redirector src/ros2/redirector/main.cpp src/ros2/redirector/node.cpp)
  ament_target_dependencies(redirector rclcpp std_msgs ethernet_msgs)

  ## File Sink node (pure ROS — no socket, no extra thread)
  add_executable(file_sink src/ros2/file_sink/main.cpp src/ros2/file_sink/node.cpp)
  ament_target_dependencies(file_sink rclcpp std_msgs ethernet_msgs)

  ## TCP Client Bridge (connection thread + reconnect)
  add_executable(tcp_client src/ros2/tcp_client/main.cpp src/ros2/tcp_client/node.cpp)
  ament_target_dependencies(tcp_client rclcpp std_msgs ethernet_msgs)
  target_link_libraries(tcp_client Threads::Threads)

  install(TARGETS udp udp_bundler redirector file_sink tcp_client DESTINATION lib/${PROJECT_NAME})

  ament_package()

else()
  message(FATAL_ERROR
    "ethernet_bridge: ROS_VERSION not set or unsupported: '$ENV{ROS_VERSION}'")
endif()
