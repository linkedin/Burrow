package security

import (
	"testing"
)

func assertBool(t *testing.T, expected, actual bool) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}

}
func TestUserChecker_AnonymousOnly(t *testing.T) {
	uc, _ := NewUserChecker(true, "Burrow", "", "USER")
	assertBool(t, true, uc.Check("", "", "GET", "/some/path"))
	assertBool(t, false, uc.Check("", "", "DELETE", "/some/path"))
	assertBool(t, false, uc.Check("user", "user", "GET", "/some/path"))
	assertBool(t, false, uc.Check("user", "user", "DELETE", "/some/path"))
}

func TestUserChecker_FileOnly(t *testing.T) {
	uc, _ := NewUserChecker(true, "Burrow", "_test/user.cfg", "")
	assertBool(t, false, uc.Check("", "", "GET", "/some/path"))
	assertBool(t, false, uc.Check("", "", "DELETE", "/some/path"))
	assertBool(t, true, uc.Check("user", "user", "GET", "/some/path"))
	assertBool(t, false, uc.Check("user", "user", "DELETE", "/some/path"))
	assertBool(t, false, uc.Check("user", "invalid", "GET", "/some/path"))
}

func TestUserChecker_FileAnonymous(t *testing.T) {
	uc, _ := NewUserChecker(true, "Burrow", "_test/user.cfg", "USER")
	assertBool(t, true, uc.Check("", "", "GET", "/some/path"))
	assertBool(t, false, uc.Check("", "", "DELETE", "/some/path"))
	assertBool(t, true, uc.Check("user", "user", "GET", "/some/path"))
	assertBool(t, false, uc.Check("user", "user", "DELETE", "/some/path"))
	assertBool(t, false, uc.Check("user", "invalid", "GET", "/some/path"))
}
