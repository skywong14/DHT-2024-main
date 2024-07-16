package chord

import (
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	MaintainInterval = 400 * time.Millisecond
	listSize         = 10
	//backUpSize = 1
)

type NodeAddr struct {
	Addr string
	Id   *big.Int
}

type DataPair struct {
	Key   string
	Value string
}

type NotifyFingerInfo struct {
	PreNode, NewNode NodeAddr
	Pos              int
}

type Node struct {
	//节点自身信息 及 对应线程锁
	addr       NodeAddr // 节点的地址和端口号
	listening  bool
	online     bool //自身是否在线
	onlineLock sync.RWMutex

	predecessor     NodeAddr
	predecessorLock sync.RWMutex
	data            map[string]string
	dataLock        sync.RWMutex
	fingerTable     [m]NodeAddr
	fingerTableLock sync.RWMutex
	fingerTablePtr  int
	//net
	listener net.Listener
	server   *rpc.Server
	//backup and successorList
	successorList     [listSize]NodeAddr
	successorListLock sync.RWMutex
	backup            map[string]string
	backupLock        sync.RWMutex
	//maintainLock
	maintainLock sync.RWMutex
	//复用
	mu   sync.Mutex
	pool map[string]*rpc.Client
}

func init() {
	initExp()
	f, _ := os.Create("[Chord]dht-test.log")
	logrus.SetOutput(f)
}

//在userdef.go/NewNode中被调用，紧跟在node:=new(chord.New)后
func (node *Node) Init(str_addr string) {
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()

	node.addr = NodeAddr{str_addr, getHash(str_addr)}
	node.fingerTablePtr = 1

	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()

	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()

	node.mu.Lock()
	node.pool = make(map[string]*rpc.Client)
	node.mu.Unlock()
}

//----来自RPC请求----
//for debug
func (node *Node) RPCPrintInfo(_ string, _ *struct{}) error {
	if node.online {
		var tmpnode NodeAddr
		node.predecessorLock.RLock()
		node.GetFirstSuccessor("", &tmpnode)
		logrus.Infof("[info] 当前节点：[%s], 前驱：[%s], 后继:[%s]        | [%s]", node.addr.Addr, node.predecessor.Addr, tmpnode.Addr, node.addr.Id)
		node.predecessorLock.RUnlock()
	} else {
		logrus.Infof("[info] 当前节点：[%s] is offline", node.addr.Addr)
	}
	return nil
}
func (node *Node) Debug() {
	node.RPCPrintInfo("", nil)
}
func (node *Node) AddInfo(info string) {
	logrus.Info(info)
}

// Ping
func (node *Node) RPCPing(_ string, _ *struct{}) error {
	if node.online {
		return nil
	}
	return errors.New("[node closed]")
}

//强制更新前驱节点
func (node *Node) RPCChange(addr NodeAddr, _ *struct{}) error {
	node.predecessorLock.Lock()
	node.predecessor = NodeAddr{addr.Addr, getHash(addr.Addr)}
	node.predecessorLock.Unlock()
	return nil
}
func (node *Node) CopyBackup(addr string, backup *(map[string]string)) error {
	*backup = make(map[string]string)
	node.dataLock.RLock()
	for key, val := range node.data {
		(*backup)[key] = val
	}
	node.dataLock.RUnlock()
	return nil
}

// 要求更新前驱节点
func (node *Node) RPCNotify(addr NodeAddr, _ *struct{}) error {
	node.predecessorLock.RLock()
	pre := node.predecessor
	node.predecessorLock.RUnlock()

	if node.predecessor.Addr == "" || between(pre.Id, node.addr.Id, addr.Id) {
		node.predecessorLock.Lock()
		node.predecessor = NodeAddr{addr.Addr, getHash(addr.Addr)}
		logrus.Infof("[Notify] [%s] is notified on [%s], new_info [%s|%s]", node.addr.Addr, addr.Addr, node.predecessor.Addr, node.predecessor.Id)
		node.predecessorLock.Unlock()
		//更新backup
		node.backupLock.Lock()
		err := node.RemoteCall(addr.Addr, "Node.CopyBackup", node.addr.Addr, &node.backup)
		node.backupLock.Unlock()
		if err != nil {
			logrus.Errorf("[Notify] error of CopyBackup, pre:[%s] error[%s]", addr.Addr, err)
			return err
		}
	}
	return nil
}

// 询问并返回前驱节点
func (node *Node) RPCGetPredecessor(_ string, reply *NodeAddr) error {
	// logrus.Info("[Lock]predecessorLock:", node.addr.Addr)
	node.predecessorLock.RLock()
	*reply = NodeAddr{node.predecessor.Addr, getHash(node.predecessor.Addr)}
	node.predecessorLock.RUnlock()
	// logrus.Info("[Unlock]predecessorLock:", node.addr.Addr)
	return nil
}

func (node *Node) RPCFindCloseSuccessor(addr NodeAddr, reply *NodeAddr) error {
	var tmp_node, suc_node NodeAddr
	err := node.RPCFindClosePredecessor(addr, &tmp_node)
	// logrus.Infof("[RPCFindCloseSuccessor step 1] cur_node[%s], find[%s] ,  %s | ID:%s", node.addr.Addr, tmp_node.Addr, tmp_node.Id, addr.Id)
	if err != nil {
		*reply = NodeAddr{"", getHash("")}
		return errors.New("[error in RPCFindCloseSuccessor][step0]")
		// return errors.New("{[error in RPCFindCloseSuccessor] with Key = [" + addr.Addr + "], Calling [" + nxt_node.Addr + "], cur_node [" + node.addr.Addr + "]}")
	}

	err = node.RemoteCall(tmp_node.Addr, "Node.GetFirstSuccessor", "", &suc_node)
	if err != nil {
		*reply = NodeAddr{"", getHash("")}
		return errors.New("[error in RPCFindCloseSuccessor][step1]")
		// return errors.New("{[error in RPCFindCloseSuccessor] with Key = [" + addr.Addr + "], Calling [" + nxt_node.Addr + "], cur_node [" + node.addr.Addr + "]}")
	}
	*reply = NodeAddr{suc_node.Addr, getHash(suc_node.Addr)} //ATTENTION::Id是个指针，应当新建而非复制
	return nil
}

func (node *Node) RPCFindClosePredecessor(addr NodeAddr, reply *NodeAddr) error {
	var suc NodeAddr
	// 获取当前节点的第一个后继节点
	err := node.GetFirstSuccessor("", &suc)
	if err != nil {
		return err
	}
	// 检查目标地址的HashId是否在当前节点和其后继节点之间
	// 如果在范围内，则当前节点即为Predecessor
	// logrus.Infof("[RPCFindClosePredecessor]  cur_node:[%s], nxt_node:[%s] ", node.addr.Addr, suc.Addr)
	if between(node.addr.Id, suc.Id, addr.Id) || cmpBigInt(suc.Id, "==", addr.Id) {
		*reply = NodeAddr{node.addr.Addr, getHash(node.addr.Addr)}
		// logrus.Infof("[1] [%s]<[%s]<=[%s]", node.addr.Addr, "...", suc.Addr)
		// logrus.Infof("[1] [%s]<[%s]<=[%s]", node.addr.Id, addr.Id, suc.Id)
		return nil
	}
	var nxt_node NodeAddr
	// 在fingerTable查找最近的前继
	node.RPCFindPrecedingFinger(addr, &nxt_node)
	err = node.RemoteCall(nxt_node.Addr, "Node.GetFirstSuccessor", "", &suc)
	suc.Id = getHash(suc.Addr)
	if err != nil {
		*reply = NodeAddr{"", getHash("")}
		logrus.Errorf("[error] [RPCFindClosePredecessor] cur_node:[%s], node_in_finger:[%s], err_info:%s ", node.addr.Addr, nxt_node.Addr, err)
		return err
	}
	// 循环模拟递归
	for !(between(nxt_node.Id, suc.Id, addr.Id) || cmpBigInt(suc.Id, "==", addr.Id)) {
		// logrus.Infof("[cycling] cur_node:[%s], nxt_node:[%s] || [%s] [%s] [%s]", nxt_node.Addr, suc.Addr, nxt_node.Id, suc.Id, addr.Id)
		// 远程调用查找更近的前继节点
		err = node.RemoteCall(nxt_node.Addr, "Node.RPCFindPrecedingFinger", addr, &nxt_node)
		if err != nil {
			*reply = NodeAddr{"", getHash("")}
			// logrus.Errorf("[error] [RPCFindClosePredecessor] state_0 递归中 cur_node:[%s], node_in_finger:[%s], err_info:%s ", nxt_node.Addr, suc.Addr, err)
			return err
		}
		err = node.RemoteCall(nxt_node.Addr, "Node.GetFirstSuccessor", "", &suc)
		if err != nil {
			*reply = NodeAddr{"", getHash("")}
			// logrus.Errorf("[error] [RPCFindClosePredecessor] state_1 递归中 cur_node:[%s], node_in_finger:[%s], err_info:%s ", nxt_node.Addr, suc.Addr, err)
			return err
		}
	}
	// logrus.Infof("[cycling] cur_node:[%s], nxt_node:[%s] || [%s] [%s] [%s]", nxt_node.Addr, suc.Addr, nxt_node.Id, suc.Id, addr.Id)
	*reply = NodeAddr{nxt_node.Addr, getHash(nxt_node.Addr)}
	return nil
}

//把map中的元素加入node.data然后清空map
func (node *Node) RPCAddData(giver map[string]string, _ *struct{}) error {
	node.dataLock.Lock()
	logrus.Infof("[1] Transferring Data to [%s], map size: %d", node.addr.Addr, len(giver))
	for key, val := range giver {
		// logrus.Infof("[1] [%s] with key [%s]", node.addr.Addr, key)
		node.data[key] = val
		//todo:备份
	}
	node.dataLock.Unlock()
	return nil
}

//返回当前在线数据中所有小于等于 preId 的数据，并把数据下线
//下线的数据加入backup，同时在suc的backup中删除
func (node *Node) RPCSplitData(pre NodeAddr, receiver *(map[string]string)) error {
	node.dataLock.Lock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	for key, val := range node.data {
		if between(getHash(key), node.addr.Id, pre.Id) || cmpBigInt(getHash(key), "==", pre.Id) {
			(*receiver)[key] = val
			delete(node.data, key)
			node.backup[key] = val
			// logrus.Infof("Transfering Data to [%s], size:[%d]", node.addr.Addr, len(*receiver))
		}
	}
	node.dataLock.Unlock()
	node.backupLock.Unlock()
	var suc_node NodeAddr
	node.GetFirstSuccessor("", &suc_node)
	// if (suc_node.Addr != node.addr.Addr)
	node.RemoteCall(suc_node.Addr, "Node.DelBackUp", node.backup, nil)

	logrus.Infof("Transfering Data to [%s], size:[%d]", node.addr.Addr, len(*receiver))

	node.predecessorLock.Lock()
	node.predecessor = NodeAddr{pre.Addr, getHash(pre.Addr)}
	node.predecessorLock.Unlock()
	return nil
}
func (node *Node) DelBackUp(data map[string]string, _ *struct{}) error {
	node.backupLock.Lock()
	for key := range data {
		_, flag := node.backup[key]
		if flag {
			delete(node.backup, key)
		}
	}
	node.backupLock.Unlock()
	return nil
}

//----自身相关----
func (node *Node) GetSuccessorList(_ string, reply *[listSize]NodeAddr) error {
	node.successorListLock.RLock()
	*reply = node.successorList
	node.successorListLock.RUnlock()
	return nil
}

func (node *Node) PutNewData(data DataPair, _ *struct{}) error {
	logrus.Infof("[Put][Data] at [%s]", node.addr.Addr)
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	node.data[data.Key] = data.Value
	return nil
}
func (node *Node) PutNewBackUp(data DataPair, _ *struct{}) error {
	logrus.Infof("[Put][Backup] at [%s]", node.addr.Addr)
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	node.backup[data.Key] = data.Value
	return nil
}
func (node *Node) CombineBackUp(data map[string]string, _ *struct{}) error {
	logrus.Infof("[Combine Backup] at [%s]", node.addr.Addr)
	node.backupLock.Lock()
	defer node.backupLock.Unlock()
	for key, val := range data {
		node.backup[key] = val
	}
	return nil
}

func (node *Node) GetFirstSuccessor(_ string, ret *NodeAddr) error {
	node.successorListLock.RLock()
	for i := 0; i < listSize; i++ {
		if node.Ping(node.successorList[i].Addr) {
			*ret = NodeAddr{node.successorList[i].Addr, getHash(node.successorList[i].Addr)}
			node.successorListLock.RUnlock()
			return nil
		}
	}
	node.successorListLock.RUnlock()
	return nil
}
func (node *Node) RPCFindPrecedingFinger(addr NodeAddr, reply *NodeAddr) error {
	node.fingerTableLock.RLock()
	defer node.fingerTableLock.RUnlock()
	for i := m - 1; i >= 0; i-- {
		if node.fingerTable[i].Addr != "" && between(node.addr.Id, addr.Id, node.fingerTable[i].Id) && node.Ping(node.fingerTable[i].Addr) {
			*reply = NodeAddr{node.fingerTable[i].Addr, getHash(node.fingerTable[i].Addr)}
			return nil
		}
	}
	*reply = NodeAddr{node.addr.Addr, getHash(node.addr.Addr)}
	return nil
}

//----涉及其他节点----

// RunRPCServer
func (node *Node) RunRPCServer() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		logrus.Error("[error] Register error: ", node.addr.Addr, err)
		return
	}
	node.listener, err = net.Listen("tcp", node.addr.Addr)
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	logrus.Infoln("[Success] Run: ", node.addr.Addr)

	for node.listening {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

// RemoteCall 远程调用
func (node *Node) RemoteCall(addrStr string, method string, args interface{}, reply interface{}) error {
	node.mu.Lock()
	client, ok := node.pool[addrStr]
	node.mu.Unlock()

	if !ok {
		var err error
		client, err = node.dial(addrStr)
		if err != nil {
			return err
		}
		node.mu.Lock()
		node.pool[addrStr] = client
		node.mu.Unlock()
	}

	err := client.Call(method, args, reply)
	if err != nil {
		logrus.Warnf("[RemoteCall] [%s] when [%s] Call [%s] [%s] [%v] ", err, node.addr.Addr, addrStr, method, args)
		node.mu.Lock()
		delete(node.pool, addrStr)
		node.mu.Unlock()
		client.Close()
	}
	return err
}
func (node *Node) dial(addrStr string) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addrStr, 150*time.Millisecond)
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)
	return client, nil
}

func (node *Node) Ping(addr_str string) bool {
	if addr_str == node.addr.Addr {
		return node.online
	}
	err := node.RemoteCall(addr_str, "Node.RPCPing", "", nil)
	if err != nil {
		return false
	} else {
		return true
	}
}

func (node *Node) UpdateSuccessorList(cur_successor NodeAddr) error {
	var suclist [listSize]NodeAddr
	err := node.RemoteCall(cur_successor.Addr, "Node.GetSuccessorList", "", &suclist)
	if err != nil {
		logrus.Errorf("[error] UpdateSuccessorList when getSuccessorList, cur_node:[%s]", node.addr.Addr)
		return err
	}
	node.successorListLock.Lock()
	node.successorList[0] = NodeAddr{cur_successor.Addr, getHash(cur_successor.Addr)}
	for i := 1; i < listSize; i++ {
		node.successorList[i] = NodeAddr{suclist[i-1].Addr, getHash(suclist[i-1].Addr)}
	}
	node.successorListLock.Unlock()
	return nil
}

// 启动节点
func (node *Node) Run() {
	node.online = true
	node.listening = true
	go node.RunRPCServer()
}

//Stablize
func (node *Node) Stablize() error {
	var cur_successor, ret NodeAddr
	node.GetFirstSuccessor("", &cur_successor)
	err := node.RemoteCall(cur_successor.Addr, "Node.RPCGetPredecessor", "", &ret)
	ret.Id = getHash(ret.Addr)
	if err != nil {
		return err
	}
	if ret.Addr != "" && between(node.addr.Id, cur_successor.Id, ret.Id) && node.Ping(ret.Addr) {
		cur_successor = ret
	}

	node.fingerTableLock.Lock()
	node.fingerTable[0] = NodeAddr{cur_successor.Addr, getHash(cur_successor.Addr)}
	node.fingerTableLock.Unlock()

	//Notify
	err = node.RemoteCall(cur_successor.Addr, "Node.RPCNotify", node.addr, nil)
	if err != nil {
		logrus.Errorf("[Notify] cur_node[%s] suc_node[%s],error: %s", node.addr.Addr, cur_successor.Addr, err)
		return err
	}

	//修复successor list
	node.UpdateSuccessorList(cur_successor)
	return nil
}

// 检测前驱节点是否在线，如果突然下线则把备份数据迁移至本地
func (node *Node) CheckPredecessor() error {
	// 检查前驱是否下线
	node.predecessorLock.RLock()
	pre := node.predecessor
	// logrus.Infof("CheckPredecessor, cur_node[%s]", pre.Addr)

	if (pre.Addr != "") && (!node.Ping(pre.Addr)) {
		node.predecessorLock.RUnlock()
		node.predecessorLock.Lock()
		logrus.Infof("[CheckPredecessor] set empty, cur_node[%s], pre[%s] state:[%b]", node.addr.Addr, pre.Addr, node.Ping(pre.Addr))
		node.predecessor = NodeAddr{"", getHash("")}
		node.predecessorLock.Unlock()
		// 备份数据
		node.backupLock.Lock()
		node.dataLock.Lock()

		// 备份数据移回主数据，并清空
		//ATTENTION可能重复
		for k, v := range node.backup {
			node.data[k] = v
		}
		var suc_node NodeAddr
		node.GetFirstSuccessor("", &suc_node)
		if suc_node.Addr != node.addr.Addr {
			node.RemoteCall(suc_node.Addr, "Node.CombineBackUp", node.backup, nil)
		}
		node.backup = make(map[string]string)
		node.dataLock.Unlock()
		node.backupLock.Unlock()

	} else {
		node.predecessorLock.RUnlock()
	}
	return nil
}

//完善FingerTable
func (node *Node) FixFinger() error {
	ptr := node.fingerTablePtr
	var ret NodeAddr
	pos := addBigInt(node.addr.Id, exp[ptr])
	err := node.RPCFindCloseSuccessor(NodeAddr{"", pos}, &ret)
	if err != nil {
		// logrus.Errorf("Error when fixingFinger, cur_node[%s]", node.addr.Addr)
		return err
	}
	// 锁定指针表锁，并更新当前指针表的地址和哈希值
	if !node.Ping(ret.Addr) {
		ret = NodeAddr{"", getHash("")}
	}
	node.fingerTableLock.Lock()
	node.fingerTable[ptr] = ret
	// logrus.Infof("[fixing] Fixing [%s].[%d] with [%s]", node.addr.Addr, ptr, ret.Addr)
	node.fingerTableLock.Unlock()
	// 更新正在修复的指针表的索引
	node.fingerTablePtr++
	if node.fingerTablePtr == m {
		node.fingerTablePtr = 1
	}
	return nil
}

//创建维护线程
func (node *Node) Maintain() {
	go func() {
		node.maintainLock.RLock()
		for node.online {
			node.Stablize()
			node.maintainLock.RUnlock()
			time.Sleep(MaintainInterval)
			node.maintainLock.RLock()
		}
		node.maintainLock.RUnlock()
		logrus.Info("[end]Stablize end: ", node.addr.Addr)
	}()
	go func() {
		for node.online {
			time.Sleep(MaintainInterval)
			// node.maintainLock.RLock()
			node.CheckPredecessor()
			// node.maintainLock.RUnlock()
		}
		logrus.Info("[end]CheckPredecessor end: ", node.addr.Addr)
	}()
	go func() {
		for node.online {
			time.Sleep(MaintainInterval)
			node.FixFinger()
		}
		logrus.Info("[end]FixFinger end: ", node.addr.Addr)
	}()
}

// 创建节点 初始化并创建一个新的 DHT 网络。
func (node *Node) Create() {
	logrus.Info("创建")

	node.predecessorLock.Lock()
	node.predecessor = NodeAddr{node.addr.Addr, getHash(node.addr.Addr)}
	node.predecessorLock.Unlock()
	node.fingerTableLock.Lock()

	node.successorListLock.Lock()
	node.successorList[0] = NodeAddr{node.addr.Addr, getHash(node.addr.Addr)}
	node.successorListLock.Unlock()

	node.fingerTable[0] = NodeAddr{node.addr.Addr, getHash(node.addr.Addr)}
	for i := 1; i < m; i++ {
		node.fingerTable[i] = NodeAddr{"", getHash("")}
	}
	node.fingerTableLock.Unlock()

	node.Maintain()
	logrus.Info("创建成功, 开始维护, 初始节点为", node.addr.Addr)
}

// 加入一个现有的 DHT 网络。从指定节点复制数据和对等节点列表，并通知所有节点新的节点加入。
func (node *Node) Join(addr_str string) bool {
	logrus.Infof("%s 加入 %s", node.addr.Addr, addr_str)
	//该节点已经下线则error
	if !node.Ping(addr_str) {
		logrus.Error("[error] Join: provided node is offline, ", node.addr.Addr, " ", addr_str)
		return false
	}

	var successor NodeAddr
	err := node.RemoteCall(addr_str, "Node.RPCFindCloseSuccessor", node.addr, &successor)
	logrus.Infof("%s has suc: %s", node.addr, successor)

	if err != nil {
		_ = node.RemoteCall(addr_str, "Node.RPCPrintInfo", "", nil)
		logrus.Errorf("[error] Join: [%s] could not find successor, with error [%s]", node.addr.Addr, err)
		return false
	}
	logrus.Info("[success] Join: ", node.addr.Addr, "->", successor.Addr)

	// 初始化前驱节点
	logrus.Info("[init] Join: init predecessor:", node.addr)
	node.predecessorLock.Lock()
	node.predecessor = NodeAddr{"", getHash("")}
	node.predecessorLock.Unlock()
	// 更新后继
	node.fingerTableLock.Lock()
	logrus.Infof("[init] Join: [%s].[0] == [%s]", node.addr.Addr, successor.Addr)
	node.fingerTable[0] = NodeAddr{successor.Addr, getHash(successor.Addr)}
	node.fingerTableLock.Unlock()

	node.dataLock.Lock()
	err = node.RemoteCall(successor.Addr, "Node.RPCSplitData", node.addr, &node.data)
	logrus.Infof("[init] Join: cur_node[%s], size [%d]", node.addr, len(node.data))

	node.dataLock.Unlock()
	if err != nil {
		logrus.Error("[error] Join and Transfer [", node.addr.Addr, "] ", err)
		return false
	}

	node.UpdateSuccessorList(successor)

	node.Maintain()
	return true
}

// 在当前节点存储一个新的键值对，并广播给所有对等节点。
func (node *Node) Put(key string, value string) bool {
	logrus.Infof("存储 %s %s", key, value)
	// 查找key的后继节点
	var suc_node, suc_suc_node NodeAddr
	err := node.RPCFindCloseSuccessor(NodeAddr{"", getHash(key)}, &suc_node)
	if err != nil {
		logrus.Error("[error] cannot find successor in Put")
		return false
	}
	// 在后继节点上存储数据
	err = node.RemoteCall(suc_node.Addr, "Node.PutNewData", DataPair{key, value}, nil)
	if err != nil {
		logrus.Errorf("[error] PutNewData in [%s]", suc_node.Addr)
		return false
	}
	// 存放备份
	err = node.RemoteCall(suc_node.Addr, "Node.GetFirstSuccessor", "", &suc_suc_node)
	if err != nil {
		logrus.Error("[error] [Put] update backup:", err)
	} else {
		err = node.RemoteCall(suc_suc_node.Addr, "Node.PutNewBackUp", DataPair{key, value}, nil)
		if err != nil {
			logrus.Error("[error] [Put] update backup:", err)
		}
	}
	// todo 备份数据

	logrus.Infof("[Success] Put: [%s] <-- (%s , %s) ", suc_node.Addr, key, value)
	return true
}

func (node *Node) RPCGetValue(key string, value *string) error {
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()
	val, flag := node.data[key]
	if flag {
		*value = val
		return nil
	} else {
		*value = ""
		return fmt.Errorf("[error] GetValue when [%s] with key [%s]", node.addr.Addr, key)
	}
}

// 从当前节点获取一个键的值。
func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("获取 %s", key)
	// 通过RPC查找key的后继节点
	var suc_node NodeAddr
	err := node.RPCFindCloseSuccessor(NodeAddr{"", getHash(key)}, &suc_node)
	if err != nil {
		logrus.Error("[error] [Get] cannot find successor")
		return false, ""
	}
	var ret_val string
	err = node.RemoteCall(suc_node.Addr, "Node.RPCGetValue", key, &ret_val)
	if err != nil {
		logrus.Error("[error] [Get] error when getting")
		return false, ""
	}
	return true, ret_val
}

func (node *Node) RPCDeleteValue(key string, _ *struct{}) error {
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()
	_, flag := node.data[key]
	if flag {
		delete(node.data, key)
		return nil
	} else {
		return fmt.Errorf("[error] DeleteValue when [%s] with key [%s]", node.addr.Addr, key)
	}
}
func (node *Node) RPCDeleteValueBackUp(key string, _ *struct{}) error {
	node.backupLock.RLock()
	defer node.backupLock.RUnlock()
	_, flag := node.backup[key]
	if flag {
		delete(node.backup, key)
		return nil
	} else {
		return fmt.Errorf("[error] DeleteValueBackUp when [%s] with key [%s]", node.addr.Addr, key)
	}
}

// 从当前节点删除一个键值对，并广播给所有对等节点。
func (node *Node) Delete(key string) bool {
	logrus.Infof("删除 %s", key)
	var suc_node, suc_suc_node NodeAddr
	err := node.RPCFindCloseSuccessor(NodeAddr{"", getHash(key)}, &suc_node)
	if err != nil {
		logrus.Error("[error] [Delete] cannot find successor")
		return false
	}
	err = node.RemoteCall(suc_node.Addr, "Node.RPCDeleteValue", key, nil)
	if err != nil {
		logrus.Error("[error] [Delete] error when deleting")
		return false
	}

	err = node.RemoteCall(suc_node.Addr, "Node.GetFirstSuccessor", "", &suc_suc_node)
	if err != nil {
		logrus.Error("[Delete] when back_up:", err)
	} else {
		err = node.RemoteCall(suc_suc_node.Addr, "Node.RPCDeleteValueBackUp", key, nil)
		if err != nil {
			logrus.Error("[Delete] when back_up:", err)
		}
	}
	return true
}

func (node *Node) turnOffNode() {
	node.listening = false
	err := node.listener.Close()
	if err != nil {
		logrus.Errorf("[error] Quit [%s], error:[%s]", node.addr.Addr, err)
	}
	node.onlineLock.Lock()
	node.online = false
	node.onlineLock.Unlock()
	logrus.Infof("[Success] Quit: [%s]", node.addr.Addr)
}

func (node *Node) RPCNotifyFinger(info NotifyFingerInfo, _ *struct{}) error {
	// logrus.Infof("[0]RPCNotifyFinger: cur_node[%s], pos:[%d], new:[%s]", node.addr.Addr, info.Pos, info.NewNode.Addr)
	node.fingerTableLock.RLock()
	addr := node.fingerTable[info.Pos]
	node.fingerTableLock.RUnlock()
	if addr.Addr == info.PreNode.Addr {
		node.fingerTableLock.Lock()
		node.fingerTable[info.Pos] = NodeAddr{info.NewNode.Addr, getHash(info.NewNode.Addr)}
		node.fingerTableLock.Unlock()
		// logrus.Infof("[1]RPCNotifyFinger: cur_node[%s], pos:[%d], new:[%s]", node.addr.Addr, info.Pos, node.fingerTable[info.Pos].Addr)
	}
	return nil
}

// 退出网络，并通知所有对等节点将自己从对等节点列表中移除。
func (node *Node) Quit() {
	logrus.Infof("退出 %s", node.addr.Addr)
	if !node.online {
		return
	}
	node.online = false
	node.maintainLock.Lock()

	//UsualQuit(update fingerTable)
	var update_node, suc_node NodeAddr
	node.GetFirstSuccessor("", &suc_node)
	for i := 0; i < m; i++ {
		pos := subBigInt(node.addr.Id, subBigInt(exp[i], big.NewInt(1)))
		err := node.RPCFindClosePredecessor(NodeAddr{"", pos}, &update_node)
		if err != nil {
			logrus.Errorf("[error] [Quit] when updating fingerTable")
		}
		if i == 0 {
			logrus.Infof("[更新] Update[%s] Pre[%s] New[%s]", update_node.Addr, node.addr.Addr, suc_node.Addr)
		}
		err = node.RemoteCall(update_node.Addr, "Node.RPCNotifyFinger", NotifyFingerInfo{PreNode: node.addr, NewNode: suc_node, Pos: i}, nil)
		if err != nil {
			logrus.Errorf("[error] [Quit] when updating fingerTable")
		}
	}

	node.dataLock.Lock()
	logrus.Infof("[2] size: %d  node: %s", len(node.data), suc_node.Addr)
	node.RemoteCall(suc_node.Addr, "Node.RPCAddData", node.data, nil)
	node.data = make(map[string]string)
	node.dataLock.Unlock()

	node.predecessorLock.RLock()
	node.RemoteCall(suc_node.Addr, "Node.RPCChange", node.predecessor, nil)
	node.predecessorLock.RUnlock()

	node.turnOffNode()
	node.maintainLock.Unlock()
	logrus.Infof("[success]退出成功 %s", node.addr.Addr)
}

// 强制退出网络，直接停止 RPC 服务器。
func (node *Node) ForceQuit() {
	logrus.Infof("%s 强制退出", node.addr.Addr)
	node.turnOffNode()
}
