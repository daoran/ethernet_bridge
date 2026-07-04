#pragma once

#include <ros/ros.h>
#include <ethernet_msgs/Packet.h>
#include <QObject>
#include <QTimer>
#include <QHostAddress>
#include <QAbstractSocket>

class QTcpSocket;
class QUdpSocket;

/**
 * ROS 1 "universal" bridge (Qt + librosqt single event loop).
 *
 * Convenience endpoint for the "talk to one named peer" case that the lean
 * tcp_client / udp nodes deliberately don't cover:
 *   - one transport, TCP *or* UDP, selected by ethernet_protocol;
 *   - the peer may be a hostname (e.g. calibrationpi.local);
 *   - with ethernet_dnsFollow the name is periodically re-resolved and a changed
 *     address is switched to even while the old one still works (TCP: reconnect,
 *     UDP: re-target the sends).
 *
 * Peer resolution uses getaddrinfo (reads /etc/hosts live, bypassing Qt's DNS
 * cache) so ethernet_dnsFollow actually observes changes; the resolved IP is
 * handed to Qt as an explicit QHostAddress. Resolution happens once at start and
 * then only on the (opt-in) ethernet_dnsFollow tick, so the default path stays
 * as lean as the sibling nodes. Unlike the udp node the send destination is the
 * configured peer, not the per-packet receiver_ip/port.
 */
class Node : public QObject
{
    Q_OBJECT

public:
    Node(ros::NodeHandle& nh, ros::NodeHandle& private_nh);
    ~Node();

private:
    // Reference to ROS Handle
    ros::NodeHandle& nh_;
    ros::NodeHandle& private_nh_;

    // ROS Interfaces
    ros::Subscriber subscriber_ethernet_;
    ros::Publisher 	publisher_ethernet_packet_;
    ros::Publisher 	publisher_ethernet_event_;

private:
    // ROS data reception callbacks
    void rosCallback_ethernet(const ethernet_msgs::Packet::ConstPtr &msg);

private:
    enum class Protocol { Tcp, Udp };

    // configuration
    struct
    {
        std::string topic_busToHost;
        std::string topic_hostToBus;
        std::string topic_event;
        std::string frame;
        Protocol    ethernet_protocol;
        std::string ethernet_peerAddress;         // hostname or IPv4
        int         ethernet_peerPort;
        std::string ethernet_bindAddress;         // UDP: local bind
        int         ethernet_bindPort;            // UDP: local bind port (0 = ephemeral)
        int         ethernet_bufferSize;
        int         ethernet_reconnectInterval;   // [ms]; <=0 disables TCP auto-reconnect
        bool        ethernet_dnsFollow;
        int         ethernet_dnsRefreshInterval;  // [ms]; re-resolve cadence when dnsFollow
    }   configuration_;

    // ethernet (exactly one is created, per ethernet_protocol)
    QTcpSocket* tcpSocket_;
    QUdpSocket* udpSocket_;

    // current resolved peer (send target for UDP + DNS-change detection)
    QHostAddress peerResolved_;
    bool         peerValid_;

    // timers
    QTimer reconnectTimer_;   // TCP reconnect (like the tcp_client)
    QTimer dnsTimer_;         // ethernet_dnsFollow re-resolve / initial-resolve retry

    bool resolvePeer(QHostAddress& out);   // getaddrinfo -> first AF_INET (live, cache-free)

private slots:
    void slotEthernetNewData();
    void slotEthernetConnected();
    void slotEthernetDisconnected();
    void slotEthernetError(QAbstractSocket::SocketError error_code);
    void slotReconnectTimer();
    void slotDnsTimer();

    // cache for runtime optimization
private:
    ethernet_msgs::Packet packet;
};
