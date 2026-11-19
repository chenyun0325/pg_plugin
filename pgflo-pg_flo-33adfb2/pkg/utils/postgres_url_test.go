package utils

import (
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPostgresURLEncodesCredentialsAndFallbackAddress(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1"} {
		c, err := pgx.ParseConfig(PostgresURL(host, 56543, "db name", "user@x", "p a'ss\\#@?"))
		require.NoError(t, err)
		require.Equal(t, "p a'ss\\#@?", c.Password)
		require.Equal(t, "user@x", c.User)
		require.Equal(t, "db name", c.Database)
		require.Equal(t, host, c.Host)
		for _, f := range c.Fallbacks {
			require.Equal(t, host, f.Host)
			require.Equal(t, uint16(56543), f.Port)
		}
	}
}
