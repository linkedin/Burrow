package security

import (
	"testing"
)

func TestAnonymousRealm(t *testing.T) {
	var realm Realm
	var user *User
	realm = &AnonymousRealm{Role: "USER"}

	user, _ = realm.Authenticate("", "")
	if user ==nil {
		t.Error("Anonymous should be authenticated")
	}
	if !user.IsAuthorized("GET", "/some/path") {
		t.Error("Anonymous should be authorized to GET /some/path")
	}
	if user.IsAuthorized("DELETE", "/some/path") {
		t.Error("Anonymous shouldn't be authorized to DELETE /some/path")
	}
}

func TestAnonymousRealm_User(t *testing.T) {
	var realm Realm
	var user *User

	realm = &AnonymousRealm{Role: "USER"}

	user, _ = realm.Authenticate("user", "password")

	if user != nil {
		t.Error("User shouldn't authenticate")
	}
}
