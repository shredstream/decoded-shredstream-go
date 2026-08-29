//go:build unix

package decodedshredstream

import (
	"net"
	"runtime"
	"syscall"
)

func recvBufferSize(conn *net.UDPConn) int {
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1
	}
	size := -1
	_ = raw.Control(func(fd uintptr) {
		if v, err := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF); err == nil {
			size = v
		}
	})
	if runtime.GOOS == "linux" && size > 0 {
		size /= 2
	}
	return size
}
