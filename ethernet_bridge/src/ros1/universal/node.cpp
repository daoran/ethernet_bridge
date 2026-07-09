#include "node.h"
#include <QTcpSocket>
#include <QUdpSocket>
#include <QNetworkDatagram>
#include <ethernet_msgs/Event.h>
#include <ethernet_msgs/EventType.h>
#include <ethernet_msgs/utils.h>

#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include <algorithm>
#include <iterator>
#include <string>


Node::Node(ros::NodeHandle& nh, ros::NodeHandle& private_nh)
    : nh_(nh), private_nh_(nh), tcpSocket_(nullptr), udpSocket_(nullptr), peerValid_(false)
{
    /// Parameter
    // Topics
    private_nh.param<std::string>("topic_busToHost", configuration_.topic_busToHost, "bus_to_host");
    private_nh.param<std::string>("topic_hostToBus", configuration_.topic_hostToBus, "host_to_bus");
    private_nh.param<std::string>("topic_event", configuration_.topic_event, "event");

    // Frame
    private_nh.param<std::string>("frame", configuration_.frame, "");

    // Transport selection
    std::string proto;
    private_nh.param<std::string>("ethernet_protocol", proto, "tcp");
    configuration_.ethernet_protocol = (proto == "udp" || proto == "UDP") ? Protocol::Udp : Protocol::Tcp;

    // Ethernet connection
    private_nh.param<std::string>("ethernet_peerAddress", configuration_.ethernet_peerAddress, "127.0.0.1");
    private_nh.param<int>("ethernet_peerPort", configuration_.ethernet_peerPort, 55555);
    private_nh.param<std::string>("ethernet_bindAddress", configuration_.ethernet_bindAddress, "0.0.0.0");
    private_nh.param<int>("ethernet_bindPort", configuration_.ethernet_bindPort, 0);
    private_nh.param<int>("ethernet_bufferSize", configuration_.ethernet_bufferSize, 0);
    private_nh.param<int>("ethernet_reconnectInterval", configuration_.ethernet_reconnectInterval, 500);
    private_nh.param<bool>("ethernet_dnsFollow", configuration_.ethernet_dnsFollow, false);
    private_nh.param<int>("ethernet_dnsRefreshInterval", configuration_.ethernet_dnsRefreshInterval, 5000);

    /// Subscribing & Publishing
    subscriber_ethernet_ = nh.subscribe(configuration_.topic_hostToBus, 100, &Node::rosCallback_ethernet, this);
    publisher_ethernet_packet_ = nh.advertise<ethernet_msgs::Packet>(configuration_.topic_busToHost, 100);
    publisher_ethernet_event_ = nh.advertise<ethernet_msgs::Event>(configuration_.topic_event, 100, true);

    /// Resolve the peer up front (getaddrinfo: live, cache-free)
    {
        QHostAddress fresh;
        if (resolvePeer(fresh)) { peerResolved_ = fresh; peerValid_ = true; }
    }

    /// Bring up socket
    if (configuration_.ethernet_protocol == Protocol::Tcp)
    {
        tcpSocket_ = new QTcpSocket(this);
        connect(tcpSocket_, SIGNAL(readyRead()), this, SLOT(slotEthernetNewData()));
        connect(tcpSocket_, SIGNAL(connected()), this, SLOT(slotEthernetConnected()));
        connect(tcpSocket_, SIGNAL(disconnected()), this, SLOT(slotEthernetDisconnected()));
#if (QT_VERSION >= QT_VERSION_CHECK(5, 15, 0))
        connect(tcpSocket_, SIGNAL(errorOccurred(QAbstractSocket::SocketError)), this, SLOT(slotEthernetError(QAbstractSocket::SocketError)));
#else
        connect(tcpSocket_, SIGNAL(error(QAbstractSocket::SocketError)), this, SLOT(slotEthernetError(QAbstractSocket::SocketError)));
#endif

        // Publish unconnected state
        slotEthernetDisconnected();

        // Configuration
        if (configuration_.ethernet_bufferSize > 0)
            tcpSocket_->setReadBufferSize(configuration_.ethernet_bufferSize);

        // Initial connection (to the explicit resolved IP, so Qt does not re-resolve/cache)
        if (peerValid_)
        {
            ROS_INFO("Connecting to %s:%u (%s) ...", configuration_.ethernet_peerAddress.data(),
                     configuration_.ethernet_peerPort, peerResolved_.toString().toLatin1().data());
            tcpSocket_->connectToHost(peerResolved_, configuration_.ethernet_peerPort);
        }
        else
            ROS_WARN("Could not resolve '%s' yet", configuration_.ethernet_peerAddress.data());

        // Timer for reconnecting on connection loss
        connect(&reconnectTimer_, SIGNAL(timeout()), this, SLOT(slotReconnectTimer()));
        if (configuration_.ethernet_reconnectInterval > 0)
        {
            reconnectTimer_.setInterval(configuration_.ethernet_reconnectInterval);
            reconnectTimer_.start();
        }
    }
    else // UDP
    {
        udpSocket_ = new QUdpSocket(this);
        connect(udpSocket_, SIGNAL(readyRead()), this, SLOT(slotEthernetNewData()));
#if (QT_VERSION >= QT_VERSION_CHECK(5, 15, 0))
        connect(udpSocket_, SIGNAL(errorOccurred(QAbstractSocket::SocketError)), this, SLOT(slotEthernetError(QAbstractSocket::SocketError)));
#else
        connect(udpSocket_, SIGNAL(error(QAbstractSocket::SocketError)), this, SLOT(slotEthernetError(QAbstractSocket::SocketError)));
#endif

        // Publish unconnected state (UDP has no real connection state)
        slotEthernetDisconnected();

        bool success = udpSocket_->bind(QHostAddress(QString::fromStdString(configuration_.ethernet_bindAddress)),
                                        configuration_.ethernet_bindPort,
                                        QAbstractSocket::ShareAddress | QUdpSocket::ReuseAddressHint);
        if (configuration_.ethernet_bufferSize > 0)
            udpSocket_->setSocketOption(QAbstractSocket::ReceiveBufferSizeSocketOption, configuration_.ethernet_bufferSize);

        ROS_INFO("Binding to %s:%u -> %s", configuration_.ethernet_bindAddress.data(),
                 configuration_.ethernet_bindPort, success ? "ok" : "failed");
        if (peerValid_)
            ROS_INFO("Peer '%s' -> %s:%u", configuration_.ethernet_peerAddress.data(),
                     peerResolved_.toString().toLatin1().data(), configuration_.ethernet_peerPort);
        else
            ROS_WARN("Could not resolve '%s' yet", configuration_.ethernet_peerAddress.data());
    }

    /// DNS-follow re-resolve, doubling as initial-resolve retry until the name is valid
    connect(&dnsTimer_, SIGNAL(timeout()), this, SLOT(slotDnsTimer()));
    if (configuration_.ethernet_dnsFollow || !peerValid_)
    {
        dnsTimer_.setInterval(configuration_.ethernet_dnsFollow ? configuration_.ethernet_dnsRefreshInterval
                                                                : std::max(1, configuration_.ethernet_reconnectInterval));
        dnsTimer_.start();
    }
}

Node::~Node()
{
    delete tcpSocket_;
    delete udpSocket_;
}

bool Node::resolvePeer(QHostAddress& out)
{
    addrinfo hints{};
    hints.ai_family = AF_INET;   // IPv4 to match the ethernet_msgs 4-byte address fields
    hints.ai_socktype = (configuration_.ethernet_protocol == Protocol::Tcp) ? SOCK_STREAM : SOCK_DGRAM;

    addrinfo* res = nullptr;
    const std::string port = std::to_string(configuration_.ethernet_peerPort);
    const int rc = ::getaddrinfo(configuration_.ethernet_peerAddress.c_str(), port.c_str(), &hints, &res);
    if (rc != 0 || res == nullptr)
    {
        if (res) ::freeaddrinfo(res);
        return false;
    }
    const sockaddr_in* sin = reinterpret_cast<const sockaddr_in*>(res->ai_addr);  // first A record
    out = QHostAddress(ntohl(sin->sin_addr.s_addr));
    ::freeaddrinfo(res);
    return true;
}

void Node::rosCallback_ethernet(const ethernet_msgs::Packet::ConstPtr &msg)
{
    if (configuration_.ethernet_protocol == Protocol::Tcp)
    {
        if (tcpSocket_->state() == QAbstractSocket::ConnectedState)
            tcpSocket_->write(reinterpret_cast<const char*>(msg->payload.data()), msg->payload.size());
        return;
    }

    // UDP: always send to the fixed, resolved peer; per-packet receiver_ip/port is
    // ignored (that's what the udp node is for).
    if (!peerValid_)
        return;
    QNetworkDatagram datagram(QByteArray(reinterpret_cast<const char*>(msg->payload.data()), msg->payload.size()),
                              peerResolved_, configuration_.ethernet_peerPort);
    udpSocket_->writeDatagram(datagram);
}

void Node::slotEthernetNewData()
{
    if (configuration_.ethernet_protocol == Protocol::Tcp)
    {
        while (tcpSocket_->bytesAvailable())
        {
            packet.header.stamp = ros::Time::now();
            packet.header.frame_id = configuration_.frame;

            packet.sender_ip = ethernet_msgs::arrayByNativeIp4(tcpSocket_->peerAddress().toIPv4Address());
            packet.sender_port = tcpSocket_->peerPort();
            packet.receiver_ip = ethernet_msgs::arrayByNativeIp4(tcpSocket_->localAddress().toIPv4Address());
            packet.receiver_port = tcpSocket_->localPort();

            packet.payload.clear();
            packet.payload.reserve(tcpSocket_->bytesAvailable());
            QByteArray payload = tcpSocket_->readAll();
            std::copy(payload.constBegin(), payload.constEnd(), std::back_inserter(packet.payload));

            publisher_ethernet_packet_.publish(packet);
        }
    }
    else // UDP
    {
        while (udpSocket_->hasPendingDatagrams())
        {
            QNetworkDatagram datagram = udpSocket_->receiveDatagram();

            packet.header.stamp = ros::Time::now();
            packet.header.frame_id = configuration_.frame;

            packet.sender_ip = ethernet_msgs::arrayByNativeIp4(datagram.senderAddress().toIPv4Address());
            packet.sender_port = datagram.senderPort();
            packet.receiver_ip = ethernet_msgs::arrayByNativeIp4(datagram.destinationAddress().toIPv4Address());
            packet.receiver_port = datagram.destinationPort();

            packet.payload = std::vector<uint8_t>(datagram.data().cbegin(), datagram.data().cend());

            publisher_ethernet_packet_.publish(packet);
        }
    }
}

void Node::slotEthernetConnected()
{
    ethernet_msgs::Event event;

    event.header.stamp = ros::Time::now();
    event.header.frame_id = configuration_.frame;

    event.type = ethernet_msgs::EventType::CONNECTED;

    publisher_ethernet_event_.publish(event);

    ROS_INFO("Connected.");
}

void Node::slotEthernetDisconnected()
{
    ethernet_msgs::Event event;

    event.header.stamp = ros::Time::now();
    event.header.frame_id = configuration_.frame;

    event.type = ethernet_msgs::EventType::DISCONNECTED;

    publisher_ethernet_event_.publish(event);

    ROS_INFO("Disconnected.");
}

void Node::slotEthernetError(QAbstractSocket::SocketError error_code)
{
    ethernet_msgs::Event event;

    event.header.stamp = ros::Time::now();
    event.header.frame_id = configuration_.frame;

    event.type = ethernet_msgs::EventType::SOCKETERROR;
    event.value = static_cast<int>(error_code);

    publisher_ethernet_event_.publish(event);

    ROS_WARN("Connection error occured, socket error code: %i", static_cast<int>(error_code));
}

void Node::slotReconnectTimer()
{
    // TCP only: reconnect once the link has fully settled to Unconnected. Guarding
    // on UnconnectedState (rather than != ConnectedState) avoids a redundant
    // connectToHost() while a connect started by slotDnsTimer() is still in
    // progress (Qt would warn "called when already looking up or connecting").
    if (peerValid_ && tcpSocket_ && tcpSocket_->state() == QAbstractSocket::UnconnectedState)
        tcpSocket_->connectToHost(peerResolved_, configuration_.ethernet_peerPort);
}

void Node::slotDnsTimer()
{
    QHostAddress fresh;
    if (!resolvePeer(fresh))
        return;  // not resolvable (yet) -> keep trying next tick

    const bool wasValid = peerValid_;
    if (wasValid && fresh == peerResolved_)
    {
        if (!configuration_.ethernet_dnsFollow)
            dnsTimer_.stop();   // this was only an initial-resolve retry
        return;
    }

    const QHostAddress old = peerResolved_;
    peerResolved_ = fresh;
    peerValid_ = true;

    if (configuration_.ethernet_protocol == Protocol::Tcp)
    {
        if (wasValid)
        {
            ROS_INFO("Peer '%s' DNS changed %s -> %s, reconnecting", configuration_.ethernet_peerAddress.data(),
                     old.toString().toLatin1().data(), fresh.toString().toLatin1().data());
            tcpSocket_->abort();   // emits disconnected() -> slotEthernetDisconnected() (DISCONNECTED event)
        }
        else
            ROS_INFO("Peer '%s' -> %s", configuration_.ethernet_peerAddress.data(), fresh.toString().toLatin1().data());

        tcpSocket_->connectToHost(peerResolved_, configuration_.ethernet_peerPort);
    }
    else // UDP: just re-target subsequent sends
    {
        if (wasValid)
            ROS_INFO("Peer '%s' DNS changed %s -> %s", configuration_.ethernet_peerAddress.data(),
                     old.toString().toLatin1().data(), fresh.toString().toLatin1().data());
        else
            ROS_INFO("Peer '%s' -> %s", configuration_.ethernet_peerAddress.data(), fresh.toString().toLatin1().data());
    }

    if (!configuration_.ethernet_dnsFollow)
        dnsTimer_.stop();   // initial-resolve retry done
}
