package security

type Realm interface {
	Authenticate(username, password string) (*User, error)
}

type User struct {
	Name string
	Roles []string
}

func (u *User) IsAuthorized(verb, path string) bool {
	var authz = false
	for _, r := range u.Roles {
		switch r {
		case "ADMIN":
			authz = true
		case "USER":
			authz = verb == "GET" || verb == "HEAD"
		}
		if authz {
			return true
		}
	}
	return authz
}

type UserChecker struct {
	Enabled bool
	RealmName string
	Realms []Realm
}

func NewUserChecker(enabled bool, userConfigFile, anonymousRole string) (uc *UserChecker,err error) {
	uc = &UserChecker{Enabled: enabled}
	if !enabled {
		return
	}
	var realm Realm
	realms := make([]Realm, 0, 2)
	// Add File Realm
	if userConfigFile != "" {
		realm, err = NewFileRealm(userConfigFile)
		if err != nil {
			return
		}
		realms = append(realms, realm)
	}
	// Add Anonymous (should always be the last one)
	if anonymousRole != "" {
		realm = &AnonymousRealm{anonymousRole}
		realms = append(realms, realm)
	}
	uc.Realms = realms
	return
}

func (uc *UserChecker) Authenticate(username, password string) (user *User, err error) {
	for _, realm := range uc.Realms {
		user, err = realm.Authenticate(username, password)
		if user != nil && err == nil {
			return
		}
	}
	return nil, err
}

func (uc *UserChecker) Check(username, password, method, path string) bool {
	user, _ := uc.Authenticate(username, password)
	if user == nil {
		return false
	}
	return user.IsAuthorized(method, path)
}
