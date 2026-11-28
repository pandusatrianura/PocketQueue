package mqtt

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pandusatrianura/pocketqueue/internal/features/broker"
	"github.com/pandusatrianura/pocketqueue/internal/features/storage"
)

// Server implements a native MQTT v3.1.1 protocol server.
type Server struct {
	addr     string
	listener net.Listener
	broker   *broker.Service
	eventBus *broker.EventBus
	logger   *slog.Logger

	mu      sync.RWMutex
	clients map[string]*Client
	stopCh  chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool

	packetIDCounter uint32
}

// NewServer instantiates an MQTT v3.1.1 protocol server.
func NewServer(addr string, brokerSvc *broker.Service, bus *broker.EventBus, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		addr:     addr,
		broker:   brokerSvc,
		eventBus: bus,
		logger:   logger.With(slog.String("protocol", "mqtt")),
		clients:  make(map[string]*Client),
		stopCh:   make(chan struct{}),
	}
}

// Addr returns the actual bound network address of the listener.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Start begins listening for incoming MQTT client connections.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	s.logger.Info("mqtt protocol server listening", slog.String("addr", ln.Addr().String()))

	s.wg.Add(1)
	go s.acceptLoop()

	if s.eventBus != nil {
		s.wg.Add(1)
		go s.eventBusPump()
	}

	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			s.logger.Error("mqtt accept error", slog.Any("error", err))
			select {
			case <-s.stopCh:
				return
			case <-time.After(50 * time.Millisecond):
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	remoteAddr := conn.RemoteAddr().String()

	// Initial timeout for CONNECT packet
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	firstPkt, err := ReadPacket(conn)
	if err != nil {
		_ = conn.Close()
		return
	}

	connectPkt, ok := firstPkt.(*ConnectPacket)
	if !ok {
		// Protocol violation: first packet MUST be CONNECT
		s.logger.Warn("first packet was not CONNECT, closing connection", slog.String("remote_addr", remoteAddr))
		_ = conn.Close()
		return
	}

	// Validate MQTT v3.1.1 protocol requirements
	if connectPkt.ProtocolName != "MQTT" || connectPkt.ProtocolLevel != 4 {
		s.logger.Warn("unsupported protocol version",
			slog.String("protocol", connectPkt.ProtocolName),
			slog.Int("level", int(connectPkt.ProtocolLevel)))
		_ = WritePacket(conn, &ConnackPacket{SessionPresent: false, ReturnCode: ConnUnacceptableProtocolVersion})
		_ = conn.Close()
		return
	}

	clientID := connectPkt.ClientID
	if clientID == "" {
		clientID = "anon-" + remoteAddr
	}

	client := NewClient(conn, clientID, connectPkt.CleanSession, connectPkt.KeepAlive)

	s.mu.Lock()
	// Replace existing connection with same clientID if present
	if old, exists := s.clients[clientID]; exists {
		_ = old.Close()
	}
	s.clients[clientID] = client
	s.mu.Unlock()

	defer func() {
		_ = client.Close()
		s.mu.Lock()
		delete(s.clients, clientID)
		s.mu.Unlock()
	}()

	// Send CONNACK: Connection Accepted
	if err := client.WritePacket(&ConnackPacket{SessionPresent: false, ReturnCode: ConnAccepted}); err != nil {
		return
	}

	// Spawn dedicated outbound writer loop for strictly ordered, non-blocking delivery
	go func() {
		for {
			select {
			case <-client.StopCh():
				return
			case pkt := <-client.Outbound():
				if err := client.WritePacket(pkt); err != nil {
					_ = client.Close()
					return
				}
			}
		}
	}()

	s.logger.Debug("mqtt client connected", slog.String("client_id", clientID), slog.String("remote_addr", remoteAddr))

	// Main packet read loop
	for {
		if s.closed.Load() {
			return
		}

		// Update read deadline based on KeepAlive
		if client.keepAlive > 0 {
			timeout := time.Duration(float64(client.keepAlive)*1.5) * time.Second
			_ = conn.SetReadDeadline(time.Now().Add(timeout))
		} else {
			_ = conn.SetReadDeadline(time.Time{})
		}

		pkt, err := ReadPacket(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			return
		}

		if err := s.dispatchPacket(client, pkt); err != nil {
			s.logger.Warn("packet dispatch error", slog.String("client_id", clientID), slog.Any("error", err))
			return
		}
	}
}

func (s *Server) dispatchPacket(client *Client, pkt Packet) error {
	switch p := pkt.(type) {
	case *PublishPacket:
		if err := ValidateTopicName(p.Topic); err != nil {
			return err
		}

		if s.broker != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := s.broker.Produce(ctx, p.Topic, 0, []byte(client.clientID), p.Payload, []storage.Header{
				{Key: "protocol", Value: []byte("mqtt")},
				{Key: "mqtt_client_id", Value: []byte(client.clientID)},
			})
			cancel()
			if err != nil {
				return err
			}
		}

		// If QoS 1, respond with PUBACK
		if p.QoS == QoS1 {
			if err := client.WritePacket(&PubackPacket{PacketID: p.PacketID}); err != nil {
				return err
			}
		}

	case *SubscribePacket:
		suback := &SubackPacket{
			PacketID:    p.PacketID,
			ReturnCodes: make([]byte, len(p.Subscriptions)),
		}

		for i, sub := range p.Subscriptions {
			if err := ValidateTopicFilter(sub.Topic); err != nil {
				suback.ReturnCodes[i] = SubackFailure
				continue
			}

			grantedQoS := sub.QoS
			if grantedQoS > QoS1 {
				grantedQoS = QoS1 // Downgrade QoS 2 to QoS 1
			}

			client.AddSubscription(sub.Topic, grantedQoS)
			suback.ReturnCodes[i] = grantedQoS
		}

		if err := client.WritePacket(suback); err != nil {
			return err
		}

	case *UnsubscribePacket:
		for _, topic := range p.Topics {
			client.RemoveSubscription(topic)
		}

		if err := client.WritePacket(&UnsubackPacket{PacketID: p.PacketID}); err != nil {
			return err
		}

	case *PingreqPacket:
		if err := client.WritePacket(&PingrespPacket{}); err != nil {
			return err
		}

	case *DisconnectPacket:
		return io.EOF // Clean client termination

	default:
		s.logger.Warn("unhandled packet type", slog.Int("type", int(pkt.Type())))
	}

	return nil
}

// eventBusPump fans out messages published to PocketQueue (from Kafka, Redis, HTTP, or MQTT)
// to all connected MQTT subscribers with matching topic filters.
func (s *Server) eventBusPump() {
	defer s.wg.Done()

	subCh, unsub := s.eventBus.Subscribe()
	defer unsub()

	for {
		select {
		case <-s.stopCh:
			return
		case rec, ok := <-subCh:
			if !ok {
				return
			}
			s.broadcastRecord(&rec)
		}
	}
}

func (s *Server) broadcastRecord(rec *storage.Record) {
	s.mu.RLock()
	var targets []*Client
	for _, c := range s.clients {
		if _, matched := c.Matches(rec.Topic); matched {
			targets = append(targets, c)
		}
	}
	s.mu.RUnlock()

	if len(targets) == 0 {
		return
	}

	packetID := uint16(atomic.AddUint32(&s.packetIDCounter, 1))

	for _, client := range targets {
		grantedQoS, _ := client.Matches(rec.Topic)
		pub := &PublishPacket{
			Topic:   rec.Topic,
			QoS:     grantedQoS,
			Payload: rec.Value,
		}
		if grantedQoS > QoS0 {
			pub.PacketID = packetID
		}

		client.QueuePacket(pub)
	}
}

// Close gracefully terminates the MQTT server and all active client connections.
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	close(s.stopCh)

	s.mu.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for _, c := range s.clients {
		_ = c.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	s.logger.Info("mqtt protocol server closed")
	return nil
}
