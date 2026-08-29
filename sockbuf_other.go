//go:build !unix

package decodedshredstream

import "net"

func recvBufferSize(_ *net.UDPConn) int { return -1 }
