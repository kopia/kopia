// Package insecureserverbind validates listen addresses for insecure, unauthenticated Kopia servers.
package insecureserverbind

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// AllowDangerousUnauthenticatedNetworkFlag is the CLI flag that disables bind restrictions.
const AllowDangerousUnauthenticatedNetworkFlag = "allow-extremely-dangerous-unauthenticated-server-on-the-network"

// AllowDangerousUnauthenticatedNetworkFlagHelp is the kingpin description for that flag.
const AllowDangerousUnauthenticatedNetworkFlagHelp = "Allow unauthenticated server to listen on non-loopback addresses; " +
	"exposes full repository and control API to the network without authentication which allows any external attacker to take full control of the server host (extremely dangerous)"

// ErrDisallowedPublicBind is returned when the address would expose an unauthenticated server beyond loopback.
var ErrDisallowedPublicBind = errors.New("refusing to expose unauthenticated server on non-loopback network bind")

// RestrictionApplies reports whether insecure unauthenticated bind checks must run.
func RestrictionApplies(insecure, withoutPassword, allowDangerousNetwork bool) bool {
	return insecure && withoutPassword && !allowDangerousNetwork
}

// ValidateListenAddressIfRestricted runs [ValidateListenAddressFlag] only when [RestrictionApplies] is true.
func ValidateListenAddressIfRestricted(insecure, withoutPassword, allowDangerousNetwork bool, address string) error {
	if !RestrictionApplies(insecure, withoutPassword, allowDangerousNetwork) {
		return nil
	}

	return ValidateListenAddressFlag(address)
}

// ValidateListenerAddrIfRestricted runs [ValidateListenerAddr] only when [RestrictionApplies] is true.
func ValidateListenerAddrIfRestricted(insecure, withoutPassword, allowDangerousNetwork bool, addr net.Addr) error {
	if !RestrictionApplies(insecure, withoutPassword, allowDangerousNetwork) {
		return nil
	}

	return ValidateListenerAddr(addr)
}

func stripProtocol(addr string) string {
	return strings.TrimPrefix(strings.TrimPrefix(addr, "https://"), "http://")
}

// ParseListenHost extracts the host part of a server listen address flag value.
// If isUnix is true, host is empty and the address refers to a Unix domain socket.
//
// Unix detection runs after stripping a leading http:// or https:// (same as the server’s
// stripProtocol). Any form that becomes unix:… is treated as a Unix socket, including:
//   - unix:/path/to/socket
//   - http://unix:/path/to/socket
//   - https://unix:/path/to/socket
func ParseListenHost(address string) (host string, isUnix bool, err error) {
	stripped := stripProtocol(address)
	if strings.HasPrefix(stripped, "unix:") {
		return "", true, nil
	}

	s := stripped
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}

	u, err := url.Parse(s)
	if err != nil {
		return "", false, fmt.Errorf("parsing listen address: %w", err)
	}

	return u.Hostname(), false, nil
}

// ValidateListenAddressFlag checks that --address is safe for an insecure server without a UI password.
func ValidateListenAddressFlag(address string) error {
	host, isUnix, err := ParseListenHost(address)
	if err != nil {
		return err
	}

	if isUnix {
		return nil
	}

	if host == "" {
		return fmt.Errorf("%w: missing host in listen address %q binds all interfaces; use loopback, a unix socket, or pass --%s (extremely dangerous)",
			ErrDisallowedPublicBind, address, AllowDangerousUnauthenticatedNetworkFlag)
	}

	if strings.EqualFold(host, "localhost") {
		return nil
	}

	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			return nil
		}

		return fmt.Errorf("%w: %q is not a loopback address; pass --%s only in isolated lab environments (extremely dangerous)",
			ErrDisallowedPublicBind, host, AllowDangerousUnauthenticatedNetworkFlag)
	}

	return fmt.Errorf("%w: hostname %q is not localhost; pass --%s only in isolated lab environments (extremely dangerous)",
		ErrDisallowedPublicBind, host, AllowDangerousUnauthenticatedNetworkFlag)
}

// ValidateListenerAddr checks the bound listener address after Listen (covers socket activation).
func ValidateListenerAddr(addr net.Addr) error {
	switch a := addr.(type) {
	case *net.UnixAddr:
		return nil
	case *net.TCPAddr:
		if a.IP != nil && a.IP.IsLoopback() {
			return nil
		}

		return fmt.Errorf("%w: listener %v is not loopback; pass --%s only in isolated lab environments (extremely dangerous)",
			ErrDisallowedPublicBind, addr, AllowDangerousUnauthenticatedNetworkFlag)
	default:
		if addr.Network() == "unix" {
			return nil
		}

		return fmt.Errorf("%w: cannot validate listener type %T %v; pass --%s only if you accept the risk",
			ErrDisallowedPublicBind, addr, addr, AllowDangerousUnauthenticatedNetworkFlag)
	}
}
