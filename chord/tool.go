package chord

import (
	"crypto/sha1"
	"math/big"

	"github.com/sirupsen/logrus"
)

const m int = 160 // m <= 160

var exp [161]*big.Int

func cmpBigInt(int1 *big.Int, symble string, int2 *big.Int) bool {
	if int1 == nil || int2 == nil {
		logrus.Errorf("[tmp]check between: %s | %s", int1.String(), int2.String())
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

func initExp() {
	for i := range exp {
		exp[i] = new(big.Int).Lsh(big.NewInt(1), uint(i))
	}
}

//在开区间内
func between(ll, rr, val *big.Int) bool {
	if ll == nil || rr == nil || val == nil {
		logrus.Errorf("[tmp]check between: %s | %s | %s", ll.String(), rr.String(), val.String())
		return true
	}
	if cmpBigInt(rr, "<=", ll) {
		return cmpBigInt(val, "<", rr) || cmpBigInt(val, ">", ll)
	} else {
		return cmpBigInt(val, "<", rr) && cmpBigInt(val, ">", ll)
	}
}

func addBigInt(int1 *big.Int, int2 *big.Int) *big.Int {
	sum := new(big.Int).Add(int1, int2)
	if cmpBigInt(sum, ">=", exp[m]) {
		sum.Sub(sum, exp[m])
	}
	return sum
}

func subBigInt(int1 *big.Int, int2 *big.Int) *big.Int {
	diff := new(big.Int).Sub(int1, int2)
	if cmpBigInt(diff, "<", big.NewInt(0)) {
		diff.Add(diff, exp[m])
	}
	return diff
}
