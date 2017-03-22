package security

import (
	"testing"
)
func TestFileRealm_User(t *testing.T) {
	var realm Realm
	var err error
	var user *User
	realm, err = NewFileRealm("_test/user.cfg", "SHA256")
	if err!=nil {
		t.Error("Invalid user config file")
	}
	user, err = realm.Authenticate("user", "user")
	if err != nil || user == nil {
		t.Error("User should be authenticated")
	}

	if !user.IsAuthorized("GET", "/some/path") {
		t.Error("User should be autorized to GET /some/path")
	}
	if user.IsAuthorized("DELETE", "/some/path") {
		t.Error("User shouldn't be autorized to DELETE /some/path")
	}
}

func TestFileRealm_UserBCrypt(t *testing.T) {
	var realm Realm
	var err error
	var user *User
	realm, err = NewFileRealm("_test/user_bcrypt.cfg", "bcrypt")
	if err!=nil {
		t.Error("Invalid user config file")
	}
	user, err = realm.Authenticate("user", "user")
	if err != nil || user == nil {
		t.Error("User should be authenticated")
	}

	if !user.IsAuthorized("GET", "/some/path") {
		t.Error("User should be autorized to GET /some/path")
	}
	if user.IsAuthorized("DELETE", "/some/path") {
		t.Error("User shouldn't be autorized to DELETE /some/path")
	}
}

func TestFileRealm_Admin(t *testing.T) {
	var realm Realm
	var err error
	var user *User
	realm, err = NewFileRealm("_test/user.cfg", "SHA256")
	if err!=nil {
		t.Error("Invalid user config file %s", err)
	}

	user, err = realm.Authenticate("admin", "admin");
	if  err != nil {
		t.Error("Admin authentication failed %s", err)
	} else if user == nil {
		t.Error("Admin should be authenticated")
	}

	if !user.IsAuthorized("GET", "/some/path") {
		t.Error("Admin should be autorized to GET /some/path")
	}
	if !user.IsAuthorized("DELETE", "/some/path") {
		t.Error("Admin should be autorized to DELETE /some/path")
	}
}

func TestFileRealm_Anonymous(t *testing.T) {
	var realm Realm
	var err error
	var user *User
	realm, err = NewFileRealm("_test/user.cfg", "SHA256")
	if err!=nil {
		t.Error("Invalid user config file")
	}

	if user, err = realm.Authenticate("", ""); err != nil && user != nil {
		t.Error("Anonymous should not be authenticated")
	}
}

func TestFileRealm_InvalidPassword(t *testing.T) {
	var realm Realm
	var err error
	var user *User
	realm, err = NewFileRealm("_test/user.cfg", "SHA256")
	if err!=nil {
		t.Error("Invalid user config file")
	}
	if user, err = realm.Authenticate("user", "invalid"); err != nil && user != nil {
		t.Error("Invalid password should not be authenticated")
	}
}
