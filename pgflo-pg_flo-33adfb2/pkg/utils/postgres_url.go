package utils

import (
	"net"
	"net/url"
	"strconv"
)

// PostgresURL encodes connection values as data, including credentials with
// spaces/quotes and IPv6 hosts. pgx still honors PGSSLMODE and certificate env.
func PostgresURL(host string, port int, database, user, password string) string {
	u := url.URL{Scheme: "postgres", Host: net.JoinHostPort(host, strconv.Itoa(port)), Path: "/" + database, User: url.UserPassword(user, password)}
	return u.String()
}
