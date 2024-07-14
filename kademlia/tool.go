package kademlia

import (
	"crypto/sha1"
	"math/big"

	"github.com/sirupsen/logrus"
)

const m int = 160 // m <= 160

func cmpBigInt(int1 *big.Int, symble string, int2 *big.Int) bool {
	if int1 == nil || int2 == nil {
		logrus.Errorf("[err]check between: %s | %s", int1.String(), int2.String())
		return true
	}
	flag := int1.Cmp(int2)
	switch symble {
	case "<":
		return (flag == -1)
	case ">":
		return (flag == 1)
	case "==":
		return (flag == 0)
	case "<=":
		return (flag != 1)
	case ">=":
		return (flag != -1)
	case "!=":
		return (flag != 0)
	}
	return false
}

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

//在开区间内
func between(ll, rr, val *big.Int) bool {
	if ll == nil || rr == nil || val == nil {
		logrus.Errorf("[err]check between: %s | %s | %s", ll.String(), rr.String(), val.String())
		return true
	}
	if cmpBigInt(rr, "<=", ll) {
		return cmpBigInt(val, "<", rr) || cmpBigInt(val, ">", ll)
	} else {
		return cmpBigInt(val, "<", rr) && cmpBigInt(val, ">", ll)
	}
}

//if cmp1距离target比cmp2更小，返回true
func closer(cmp1, cmp2, target *big.Int) bool {
	dist1 := Xor(cmp1, target)
	dist2 := Xor(cmp2, target)
	return dist1.Cmp(dist2) < 0
}
