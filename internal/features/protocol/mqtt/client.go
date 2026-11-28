package mqtt

import (
	"net"
	"sync"
)

// Client represents a connected MQTT client session.
type Client struct {
	conn         net.Conn
	clientID     string
	cleanSession bool
	keepAlive    uint16
	sendCh       chan Packet
	stopCh       chan struct{}

	mu            sync.Mutex
	subscriptions map[string]byte // topicFilter -> grantedQoS
	closed        bool
}

// NewClient creates a new Client session.
func NewClient(conn net.Conn, clientID string, cleanSession bool, keepAlive uint16) *Client {
	return &Client{
		conn:          conn,
		clientID:      clientID,
		cleanSession:  cleanSession,
		keepAlive:     keepAlive,
		sendCh:        make(chan Packet, 256),
		stopCh:        make(chan struct{}),
		subscriptions: make(map[string]byte),
	}
}

// StopCh returns the notification channel closed when the client disconnects.
func (c *Client) StopCh() <-chan struct{} {
	return c.stopCh
}

// ClientID returns the client's identifier.
func (c *Client) ClientID() string {
	return c.clientID
}

// RemoteAddr returns the remote network address.
func (c *Client) RemoteAddr() net.Addr {
	if c.conn != nil {
		return c.conn.RemoteAddr()
	}
	return nil
}

// WritePacket serializes and sends an MQTT control packet to this client in a thread-safe manner.
func (c *Client) WritePacket(p Packet) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return net.ErrClosed
	}

	return WritePacket(c.conn, p)
}

// QueuePacket queues an outbound packet for sequential delivery without blocking the caller.
func (c *Client) QueuePacket(p Packet) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return false
	}

	select {
	case c.sendCh <- p:
		return true
	default:
		return false
	}
}

// Outbound returns the channel of queued outbound packets.
func (c *Client) Outbound() <-chan Packet {
	return c.sendCh
}

// AddSubscription registers a topic filter with granted QoS.
func (c *Client) AddSubscription(filter string, qos byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscriptions[filter] = qos
}

// RemoveSubscription removes a topic filter.
func (c *Client) RemoveSubscription(filter string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscriptions, filter)
}

// Subscriptions returns a copy of the client's topic subscriptions.
func (c *Client) Subscriptions() map[string]byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	subs := make(map[string]byte, len(c.subscriptions))
	for f, q := range c.subscriptions {
		subs[f] = q
	}
	return subs
}

// Matches checks if any of the client's subscriptions match the given topic name.
// Returns (grantedQoS, matched).
func (c *Client) Matches(topic string) (byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var (
		matched    bool
		highestQoS byte = QoS0
	)

	for filter, qos := range c.subscriptions {
		if MatchTopic(filter, topic) {
			matched = true
			if qos > highestQoS {
				highestQoS = qos
			}
		}
	}

	return highestQoS, matched
}

// Close terminates the client connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	close(c.stopCh)

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
