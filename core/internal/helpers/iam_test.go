package helpers

import (
	"context"
	"errors"
	"testing"

	sarama "github.com/IBM/sarama"
)

// stubAll replaces the three signer helpers and guarantees they are restored.
func stubAll(
	t *testing.T,
	authFn func(ctx context.Context, region string) (string, int64, error),
	roleFn func(ctx context.Context, region, roleArn, session string) (string, int64, error),
	profFn func(ctx context.Context, region, profile string) (string, int64, error),
) func() {
	origAuth := signerGenerateAuthToken
	origRole := signerGenerateAuthTokenFromRole
	origProf := signerGenerateAuthTokenFromProfile

	signerGenerateAuthToken = authFn
	signerGenerateAuthTokenFromRole = roleFn
	signerGenerateAuthTokenFromProfile = profFn

	return func() {
		signerGenerateAuthToken = origAuth
		signerGenerateAuthTokenFromRole = origRole
		signerGenerateAuthTokenFromProfile = origProf
	}
}

// helpers that must *not* be invoked in a given path
func mustNotCallAuth(t *testing.T) func(ctx context.Context, region string) (string, int64, error) {
	return func(context.Context, string) (string, int64, error) {
		t.Fatalf("unexpected call to GenerateAuthToken")
		return "", 0, nil
	}
}
func mustNotCallRole(t *testing.T) func(context.Context, string, string, string) (string, int64, error) {
	return func(context.Context, string, string, string) (string, int64, error) {
		t.Fatalf("unexpected call to GenerateAuthTokenFromRole")
		return "", 0, nil
	}
}
func mustNotCallProf(t *testing.T) func(context.Context, string, string) (string, int64, error) {
	return func(context.Context, string, string) (string, int64, error) {
		t.Fatalf("unexpected call to GenerateAuthTokenFromProfile")
		return "", 0, nil
	}
}

func TestIamTokenProvider(t *testing.T) {
	tests := []struct {
		name      string
		provider  iamTokenProvider
		setupStub func(t *testing.T) (restore func())
		wantTok   string
		wantErr   bool
	}{
		{
			name:     "default credential chain",
			provider: iamTokenProvider{region: "eu-central-1"},
			setupStub: func(t *testing.T) func() {
				return stubAll(
					t,
					func(_ context.Context, region string) (string, int64, error) {
						if region != "eu-central-1" {
							t.Fatalf("region = %s, want eu-central-1", region)
						}
						return "tok-default", 0, nil
					},
					mustNotCallRole(t),
					mustNotCallProf(t),
				)
			},
			wantTok: "tok-default",
		},
		{
			name:     "assume‑role path overrides profile",
			provider: iamTokenProvider{region: "us-east-1", roleArn: "arn:aws:iam::123456789012:role/test", profile: "ignored"},
			setupStub: func(t *testing.T) func() {
				return stubAll(
					t,
					mustNotCallAuth(t),
					func(_ context.Context, region, role, sess string) (string, int64, error) {
						if region != "us-east-1" || role != "arn:aws:iam::123456789012:role/test" {
							t.Fatalf("bad args to role helper")
						}
						return "tok-role", 0, nil
					},
					mustNotCallProf(t),
				)
			},
			wantTok: "tok-role",
		},
		{
			name:     "named profile path",
			provider: iamTokenProvider{region: "ap-south-1", profile: "burrow"},
			setupStub: func(t *testing.T) func() {
				return stubAll(
					t,
					mustNotCallAuth(t),
					mustNotCallRole(t),
					func(_ context.Context, region, profile string) (string, int64, error) {
						if region != "ap-south-1" || profile != "burrow" {
							t.Fatalf("bad args to profile helper")
						}
						return "tok-profile", 0, nil
					},
				)
			},
			wantTok: "tok-profile",
		},
		{
			name:     "signer returns error",
			provider: iamTokenProvider{region: "eu-west-1"},
			setupStub: func(t *testing.T) func() {
				return stubAll(
					t,
					func(context.Context, string) (string, int64, error) {
						return "", 0, errors.New("boom")
					},
					mustNotCallRole(t),
					mustNotCallProf(t),
				)
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			restore := tc.setupStub(t)
			defer restore()

			got, err := tc.provider.Token()

			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil || got.Token != tc.wantTok {
				t.Fatalf("token = %v, want %s", got, tc.wantTok)
			}
			if _, ok := interface{}(got).(*sarama.AccessToken); !ok {
				t.Fatalf("return type is not *sarama.AccessToken")
			}
		})
	}
}
