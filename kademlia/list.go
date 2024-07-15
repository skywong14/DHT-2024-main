package kademlia

import (
	"container/list"
	"math/big"
	"strings"
	"sync"
)

type KListSlice struct {
	Elements []string
	CurNode  string
}

func FromKListSlice(slice KListSlice) KList {
	l := KList{
		L:       list.New(),
		CurNode: slice.CurNode,
	}

	for _, elem := range slice.Elements {
		l.L.PushBack(elem)
	}

	return l
}
func (l *KList) ToKListSlice() KListSlice {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	elements := make([]string, 0, l.L.Len())
	for e := l.L.Front(); e != nil; e = e.Next() {
		if val, ok := e.Value.(string); ok {
			elements = append(elements, val)
		}
	}
	return KListSlice{
		Elements: elements,
		CurNode:  l.CurNode,
	}
}

type KList struct {
	L       *list.List
	CurNode string
	Mu      sync.RWMutex
}

func NewListPtr(addrStr string) *KList {
	return &KList{
		L:       list.New(),
		CurNode: addrStr,
	}
}

func NewList(addrStr string) KList {
	return KList{
		L:       list.New(),
		CurNode: addrStr,
	}
}

func (l *KList) Init(addrStr string) {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	l.L = list.New()
	l.CurNode = addrStr
}

func (l *KList) Print() string {
	l.Mu.Lock()
	defer l.Mu.Unlock()

	var sb strings.Builder
	for e := l.L.Front(); e != nil; e = e.Next() {
		str, _ := e.Value.(string)
		sb.WriteString("|")
		sb.WriteString(str)
	}
	return sb.String()
}

func (l *KList) PushBack(val interface{}) {
	l.Mu.Lock()
	l.L.PushBack(val)
	l.Mu.Unlock()
}

func (l *KList) Size() int {
	l.Mu.RLock()
	defer l.Mu.RUnlock()
	return l.L.Len()
}
func (l *KList) Find(val interface{}) *list.Element {
	l.Mu.RLock()
	defer l.Mu.RUnlock()
	for e := l.L.Front(); e != nil; e = e.Next() {
		if e.Value == val {
			return e
		}
	}
	return nil
}

func (l *KList) Remove(val string) bool {
	e := l.Find(val)
	l.Mu.Lock()
	defer l.Mu.Unlock()
	if e == nil {
		return false
	}
	l.L.Remove(e)
	return true
}
func (l *KList) RemovePtr(e *list.Element) bool {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	if e == nil {
		return false
	}
	l.L.Remove(e)
	return true
}

//传入当前节点的指针和update的值
func (l *KList) UpdateBucket(curNodePtr *Node, val string) bool {
	//不能返回CurNode
	if val == l.CurNode {
		return false
	}
	if !curNodePtr.Ping(val) {
		return false
	}
	ePtr := l.Find(val)
	if ePtr != nil {
		// 该元素已经存在
		l.Mu.Lock()
		l.L.Remove(ePtr)
		l.L.PushBack(val)
		l.Mu.Unlock()
		return true
	} else {
		if l.Size() < kSize {
			l.PushBack(val)
			return true
		} else {
			l.Mu.RLock()
			ePtr = l.L.Front()
			l.Mu.RUnlock()
			var headNode string = ePtr.Value.(string)
			if curNodePtr.Ping(headNode) {
				l.RemovePtr(ePtr)
				l.PushBack(headNode)
				return false
			} else {
				l.RemovePtr(ePtr)
				l.PushBack(val)
				return true
			}
		}
	}
	return false
}

//按距离更新
func (l *KList) UpdateKList(curNodePtr *Node, val string) bool {
	//不能返回CurNode
	if val == l.CurNode {
		return false
	}
	if !curNodePtr.Ping(val) {
		return false
	}
	ePtr := l.Find(val)
	if ePtr != nil {
		// 该元素已经存在
		return true
	} else {
		if l.Size() < kSize {
			l.PushBack(val)
			return true
		} else {
			//删去距离CurNode最远的节点
			var strValue string
			ePtr = nil
			var tmpHash *big.Int
			targetHash := getHash(l.CurNode)

			l.Mu.Lock()
			//获取距离最远的节点
			for e := l.L.Front(); e != nil; e = e.Next() {
				strValue, _ = e.Value.(string)
				if ePtr == nil || closer(tmpHash, getHash(strValue), targetHash) {
					ePtr = e
					tmpHash = getHash(strValue)
				}
			}
			//跟val比较
			if closer(getHash(val), tmpHash, targetHash) {
				l.L.Remove(ePtr)
				l.L.PushBack(val)
				l.Mu.Unlock()
				return true
			} else {
				l.Mu.Unlock()
				return false
			}
		}
	}
	return false
}
