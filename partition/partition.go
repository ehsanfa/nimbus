package partition

import (
	"crypto/sha1"
	"math/big"
)

func getToken(k string) int64 {
	hash := sha1.New()
	hash.Write([]byte(k))
	hashBytes := hash.Sum(nil)
	hashInt := new(big.Int).SetBytes(hashBytes)
	return hashInt.Mod(hashInt, big.NewInt(180)).Int64() + 1
}
