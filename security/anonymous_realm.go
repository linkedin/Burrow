package security

type AnonymousRealm struct {
	Role string
}

func (r *AnonymousRealm) Authenticate(username, password string) (*User, error) {
	if username != "" {
		return nil, nil
	}
	return &User{Roles:[]string{r.Role}}, nil
}
