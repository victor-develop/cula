package codex

import (
	"testing"

	cula "github.com/git-hulk/cula/pkg"
)

func TestAuthStatusFromFile(t *testing.T) {
	tests := []struct {
		name string
		data string
		want cula.AuthStatus
	}{
		{
			name: "oauth tokens",
			data: `{"tokens":{"access_token":"abc"}}`,
			want: cula.AuthLoggedIn,
		},
		{
			name: "api key mode",
			data: `{"auth_mode":"apikey","OPENAI_API_KEY":"sk-xxx"}`,
			want: cula.AuthLoggedIn,
		},
		{
			name: "empty tokens and key",
			data: `{"tokens":{"access_token":""},"OPENAI_API_KEY":""}`,
			want: cula.AuthLoggedOut,
		},
		{
			name: "empty object",
			data: `{}`,
			want: cula.AuthLoggedOut,
		},
		{
			name: "invalid json",
			data: `not json`,
			want: cula.AuthLoggedOut,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := authStatusFromFile([]byte(tt.data)); got != tt.want {
				t.Fatalf("authStatusFromFile(%q) = %v, want %v", tt.data, got, tt.want)
			}
		})
	}
}
