package partition

import (
	"crypto/sha1"
	"math/big"
)

func GetToken(k string, maxParts int64) int64 {
	if maxParts <= 0 {
		panic("maximum number of partitions cannot be smaller than 1")
	}
	hash := sha1.New()
	hash.Write([]byte(k))
	hashBytes := hash.Sum(nil)
	hashInt := new(big.Int).SetBytes(hashBytes)
	return hashInt.Mod(hashInt, big.NewInt(maxParts)).Int64() + 1
}

/*
Suggests the partition for a new node
numberOfNodes reprents how many nodes are already in the cluster
*/
func SuggestPartition(maxParts int64, numberOfNodes int64) int64 {
	if numberOfNodes == 0 {
		return 0
	}

	var right, left int64

	right = 0
	left = 360

	return (left + right) / 2
}
