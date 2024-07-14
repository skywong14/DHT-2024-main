package kademlia

import (
	"container/list"
	"math/big"
	"strings"
	"sync"
)

type kList struct {
	mu       sync.RWMutex
	l        *list.List
	cur_node string
}

func NewListPtr(addr_str string) *kList {
	return &kList{
		l:        list.New(),
		cur_node: addr_str,
	}
}
func NewList(addr_str string) kList {
	return kList{
		l:        list.New(),
		cur_node: addr_str,
	}
}
func (l *kList) Init(addr_str string) {
	l.mu.Lock()
	l.l = list.New()
	l.cur_node = addr_str
	l.mu.Unlock()
}

func (l *kList) Print() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	var sb strings.Builder
	for e := l.l.Front(); e != nil; e = e.Next() {
		str, _ := e.Value.(string)
		sb.WriteString("|")
		sb.WriteString(str)
	}
	return sb.String()
}

func (l *kList) PushBack(val interface{}) {
	l.mu.Lock()
	l.l.PushBack(val)
	l.mu.Unlock()
}

func (l *kList) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.l.Len()
}
func (l *kList) Find(val interface{}) *list.Element {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for e := l.l.Front(); e != nil; e = e.Next() {
		if e.Value == val {
			return e
		}
	}
	return nil
}

func (l *kList) Remove(val string) bool {
	e := l.Find(val)
	l.mu.Lock()
	defer l.mu.Unlock()
	if e == nil {
		return false
	}
	l.l.Remove(e)
	return true
}
func (l *kList) RemovePtr(e *list.Element) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e == nil {
		return false
	}
	l.l.Remove(e)
	return true
}

//传入当前节点的指针和update的值
func (l *kList) UpdateBucket(cur_node_ptr *Node, val string) bool {
	if !cur_node_ptr.Ping(val) {
		return false
	}
	e_ptr := l.Find(val)
	if e_ptr != nil {
		// 该元素已经存在
		l.mu.Lock()
		l.l.Remove(e_ptr)
		l.l.PushBack(val)
		l.mu.Unlock()
		return true
	} else {
		if l.Size() < kSize {
			l.PushBack(val)
			return true
		} else {
			l.mu.RLock()
			e_ptr = l.l.Front()
			l.mu.RUnlock()
			var head_node string = e_ptr.Value.(string)
			if cur_node_ptr.Ping(head_node) {
				l.RemovePtr(e_ptr)
				l.PushBack(head_node)
				return false
			} else {
				l.RemovePtr(e_ptr)
				l.PushBack(val)
				return true
			}
		}
	}
	return false
}

//按距离更新
func (l *kList) UpdateKList(cur_node_ptr *Node, val string) bool {
	if !cur_node_ptr.Ping(val) {
		return false
	}
	e_ptr := l.Find(val)
	if e_ptr != nil {
		// 该元素已经存在
		return true
	} else {
		if l.Size() < kSize {
			l.PushBack(val)
			return true
		} else {
			//删去距离cur_node最远的节点
			var strValue string
			e_ptr = nil
			var tmp_hash *big.Int
			target_hash := getHash(l.cur_node)

			l.mu.Lock()
			//获取距离最远的节点
			for e := l.l.Front(); e != nil; e = e.Next() {
				strValue, _ = e.Value.(string)
				if e_ptr == nil || closer(tmp_hash, getHash(strValue), target_hash) {
					e_ptr = e
					tmp_hash = getHash(strValue)
				}
			}
			//跟val比较
			if closer(getHash(val), tmp_hash, target_hash) {
				l.l.Remove(e_ptr)
				l.l.PushBack(val)
				l.mu.Unlock()
				return true
			} else {
				l.mu.Unlock()
				return false
			}
		}
	}
	return false
}
