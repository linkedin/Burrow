package shims

import (
	"crypto/subtle"
	"crypto/x509"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

// HerokuVerifyPeerFunc is passed into the sarama client TLS config in order to verify the server's
// certificate properly. This is required because in order to connect to Heroku Kafka we have to set
// InsecureSkipVerify to true as Heroku Kafka certificate hostnames do not match the cluster hostname
// so we can't rely on default certificate verification.
//
// The second argument to the verification function is always `nil` if InsecureSkipVerify is true.
//
// See: https://devcenter.heroku.com/articles/kafka-on-heroku#using-kafka-in-go-applications
// See: https://pkg.go.dev/crypto/tls
func HerokuVerifyPeerFunc(ca *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(certs [][]byte, _ [][]*x509.Certificate) error {
		opts := x509.VerifyOptions{Roots: ca}

		for _, raw := range certs {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				return err
			}

			if _, err = cert.Verify(opts); err != nil {
				return err
			}
		}

		return nil
	}
}

// secureCmp does constant time comparison of two byte slices
// returning true if they are identical
func secureCmp(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}

// BasicAuthMiddleware protects the given handler with basic authentication
func BasicAuthMiddleware(next http.Handler, user, pass []byte) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			username, password, ok := r.BasicAuth()

			if ok && secureCmp(user, []byte(username)) && secureCmp(pass, []byte(password)) {
				next.ServeHTTP(w, r)
			} else {
				w.Header().Set("WWW-Authenticate", `Basic realm=Access to Burrow"`)
				w.WriteHeader(401)
				w.Write([]byte("Unauthorized.\n"))
			}
		},
	)
}

func ApplyPeerVerification(saramaConfig *sarama.Config, ca *x509.CertPool) {
	saramaConfig.Net.TLS.Config.VerifyPeerCertificate = HerokuVerifyPeerFunc(ca)
}

func ApplyBasicAuthMiddleware(httpServerConfigName string, next http.Handler) http.Handler {
	username := viper.GetString(httpServerConfigName + ".basic-auth-username")
	password := viper.GetString(httpServerConfigName + ".basic-auth-password")

	if username == "" || password == "" {
		return next
	}

	return BasicAuthMiddleware(next, []byte(username), []byte(password))
}
