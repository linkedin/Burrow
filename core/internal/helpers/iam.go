package helpers

import (
	"context"

	sarama "github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
)

var (
	signerGenerateAuthToken            = signer.GenerateAuthToken
	signerGenerateAuthTokenFromRole    = signer.GenerateAuthTokenFromRole
	signerGenerateAuthTokenFromProfile = signer.GenerateAuthTokenFromProfile
)

type iamTokenProvider struct {
	region, roleArn, profile string
}

func (p *iamTokenProvider) Token() (*sarama.AccessToken, error) {
	var tok string
	var err error

	switch {
	case p.roleArn != "":
		tok, _, err = signerGenerateAuthTokenFromRole(
			context.TODO(), p.region, p.roleArn, "burrow-session")

	case p.profile != "":
		tok, _, err = signerGenerateAuthTokenFromProfile(
			context.TODO(), p.region, p.profile)

	default:
		tok, _, err = signerGenerateAuthToken(
			context.TODO(), p.region)
	}

	if err != nil {
		return nil, err
	}
	return &sarama.AccessToken{Token: tok}, nil
}
