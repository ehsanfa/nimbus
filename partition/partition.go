package partition

import (
	"crypto/sha256"
	"math"
	"math/big"
	"math/rand"
	"time"
)

type Token int64

func GetToken(k []byte) Token {
	hash := sha256.New()
	hash.Write(k)
	hashBytes := hash.Sum(nil)
	hashInt := new(big.Int).SetBytes(hashBytes)
	return Token(hashInt.Mod(hashInt, big.NewInt(1<<32/2-1)).Int64() + 1)
}

func (t Token) Less(than Token) bool {
	return t < than
}

/*
Suggests the partition for a new node
numberOfNodes reprents how many nodes are already in the cluster
*/
func SuggestToken() Token {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return Token(rnd.Intn(math.MaxInt32))
}
