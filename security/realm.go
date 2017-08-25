package security

import (
	log "github.com/cihub/seelog"
)

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

func NewUserChecker(enabled bool, realmName, userConfigFile, userPasswordHash, anonymousRole string) (uc *UserChecker,err error) {
	uc = &UserChecker{Enabled: enabled, RealmName: realmName}
	if !enabled {
		return
	}
	var realm Realm
	realms := make([]Realm, 0, 2)
	// Add File Realm
	if userConfigFile != "" {
		log.Infof("HTTP Basic Auth, Initializing File Realm from %s", userConfigFile)
		realm, err = NewFileRealm(userConfigFile, userPasswordHash)
		if err != nil {
			return
		}
		realms = append(realms, realm)
	}
	// Add Anonymous (should always be the last one)
	if anonymousRole != "" {
		log.Infof("HTTP Basic Auth, Initializing Anonymous Realm with role %s", anonymousRole)
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
	user, err := uc.Authenticate(username, password)
	if err != nil {
		log.Warnf("User %s authentication failed with error %v", username, err)
		return false
	}
	if user == nil {
		return false
	}
	return user.IsAuthorized(method, path)
}
