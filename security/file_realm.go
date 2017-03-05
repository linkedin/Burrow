package security

import (
	"golang.org/x/crypto/bcrypt"
	//"errors"
	"gopkg.in/gcfg.v1"
)

type FileRealm struct {
	Config UserConfig
}

type FileUser struct {
	Password string   `gcfg:"password"`
	Role     []string `gcfg:"role"`
}

type UserConfig struct {
	User map[string]*FileUser
}

func NewFileRealm(userConfigFile string) (*FileRealm, error) {
	realm := &FileRealm{}
	err := gcfg.ReadFileInto(&realm.Config, userConfigFile)
	if err != nil {
		return nil, err
	}
	return realm, nil
}

func (r *FileRealm) Authenticate(username, password string) (*User, error) {
	if username == "" {
		return nil, nil
	}
	user, ok := r.Config.User[username]
	if !ok {
		return nil, nil
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
		return nil, err
	}
	//if user.Password != password {
	//	return nil, errors.New("Invalid password")
	//}
	return &User{Name: username, Roles: user.Role}, nil
}
