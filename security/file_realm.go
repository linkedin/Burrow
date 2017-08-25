package security

import (
	"golang.org/x/crypto/bcrypt"
	"crypto"
	"encoding/hex"
	"gopkg.in/gcfg.v1"
	"errors"
	"strings"
	"fmt"
)

type FileRealm struct {
	Config UserConfig
	HashFunc string
	passwordVerifier passwordVerifier
}

type FileUser struct {
	Password string   `gcfg:"password"`
	Role     []string `gcfg:"role"`
}

type UserConfig struct {
	User map[string]*FileUser
}

type passwordVerifier interface {
	compareHashAndPassword(hash string, password string) error
}

type cryptoPasswordVerifier struct {
	hash crypto.Hash
}

func (v *cryptoPasswordVerifier) compareHashAndPassword(hash string, password string) error {
	hashBuilder := v.hash.New()
	hashBuilder.Write([]byte(password))
	hashedPassword := hex.EncodeToString(hashBuilder.Sum(nil))
	if hashedPassword != hash {
		return errors.New("Invalid password")
	}
	return nil
}

type bcryptPasswordVerifier struct {

}

func (v *bcryptPasswordVerifier) compareHashAndPassword(hash string, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

func NewFileRealm(userConfigFile, hashFunc string) (*FileRealm, error) {
	realm := &FileRealm{}
	err := gcfg.ReadFileInto(&realm.Config, userConfigFile)
	if err != nil {
		return nil, err
	}
	hashFunc = strings.ToLower(hashFunc)
	switch hashFunc {
	case "bcrypt":
		realm.passwordVerifier = &bcryptPasswordVerifier{}
	default:
		hashIds := map[string]crypto.Hash{
			"md5": crypto.MD5,
			"sha1": crypto.SHA1,
			"sha224":   crypto.SHA224,
			"sha256": crypto.SHA256,
			"sha512": crypto.SHA512,
			"sha512_224": crypto.SHA512_224,
			"sha512_256": crypto.SHA512_256,
		}
		hashId,ok := hashIds[hashFunc]
		if !ok {
			return nil, errors.New(fmt.Sprintf("Invalid Hash function %s", hashFunc))
		}
		realm.passwordVerifier = &cryptoPasswordVerifier{crypto.Hash(hashId)}
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
	if err := r.passwordVerifier.compareHashAndPassword(user.Password, password); err != nil {
		return nil, err
	}
	//if user.Password != password {
	//	return nil, errors.New("Invalid password")
	//}
	return &User{Name: username, Roles: user.Role}, nil
}
