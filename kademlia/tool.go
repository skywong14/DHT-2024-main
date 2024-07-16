package kademlia

import (
	"crypto/sha1"
	"math/big"
)

const m int = 160 // m <= 160

func getHash(str string) *big.Int {
	ha := sha1.New()
	ha.Write([]byte(str))
	return new(big.Int).SetBytes(ha.Sum(nil))
}

func Xor(int1, int2 *big.Int) *big.Int {
	result := new(big.Int)
	result.Xor(int1, int2)
	return result
}
func cpl(int1, int2 *big.Int) int {
	dis := Xor(int1, int2)
	return dis.BitLen() - 1
}

//if cmp1距离target比cmp2更小，返回true
func closer(cmp1, cmp2, target *big.Int) bool {
	dist1 := Xor(cmp1, target)
	dist2 := Xor(cmp2, target)
	return dist1.Cmp(dist2) < 0
}
