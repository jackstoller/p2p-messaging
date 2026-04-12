package stun

import (
	"encoding/binary"
	"fmt"
	"net"
)

const (
	bindingRequestType = 0x0001
	successResponse    = 0x0101
	magicCookie        = 0x2112A442
	headerSize         = 20
	attrXorMappedAddr  = 0x0020
)

// Server is a minimal RFC5389 STUN binding responder for ICE host/srflx discovery.
type Server struct {
	conn *net.UDPConn
}

func Listen(addr string) (*Server, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("resolve stun addr: %w", err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("listen stun udp: %w", err)
	}
	return &Server{conn: conn}, nil
}

func (s *Server) Serve() error {
	buf := make([]byte, 1500)
	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return err
		}
		if response, ok := buildResponse(buf[:n], addr); ok {
			_, _ = s.conn.WriteToUDP(response, addr)
		}
	}
}

func (s *Server) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

func buildResponse(packet []byte, addr *net.UDPAddr) ([]byte, bool) {
	if len(packet) < headerSize {
		return nil, false
	}
	if binary.BigEndian.Uint16(packet[0:2]) != bindingRequestType {
		return nil, false
	}
	if binary.BigEndian.Uint32(packet[4:8]) != magicCookie {
		return nil, false
	}
	ip4 := addr.IP.To4()
	if ip4 == nil {
		return nil, false
	}

	response := make([]byte, headerSize+12)
	binary.BigEndian.PutUint16(response[0:2], successResponse)
	binary.BigEndian.PutUint16(response[2:4], 12)
	binary.BigEndian.PutUint32(response[4:8], magicCookie)
	copy(response[8:20], packet[8:20])

	binary.BigEndian.PutUint16(response[20:22], attrXorMappedAddr)
	binary.BigEndian.PutUint16(response[22:24], 8)
	response[24] = 0
	response[25] = 0x01
	binary.BigEndian.PutUint16(response[26:28], uint16(addr.Port)^(magicCookie>>16))
	cookie := make([]byte, 4)
	binary.BigEndian.PutUint32(cookie, magicCookie)
	for i := 0; i < 4; i += 1 {
		response[28+i] = ip4[i] ^ cookie[i]
	}
	return response, true
}
