package kademlia

import (
	"errors"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	kSize             int = 15
	alpha             int = 3
	RepublishInterval     = 1500 * time.Millisecond
)

type DataPair struct {
	Key   string
	Value string
}

type Node struct {
	//节点自身信息 及 对应线程锁
	addr      string // 节点的地址和端口号，与对应Hash
	id        *big.Int
	listening bool
	online    bool // 自身是否在线

	data     map[string]string
	dataLock sync.RWMutex
	//net
	listener net.Listener
	server   *rpc.Server
	//复用
	mu   sync.Mutex
	pool map[string]*rpc.Client
	//kBucket
	kBucket [m]KList
}

func init() {
	f, _ := os.Create("[Kademlia]dht-test.log")
	logrus.SetOutput(f)
}

//在userdef.go/NewNode中被调用，紧跟在node:=new(chord.New)后
func (node *Node) Init(str_addr string) {
	node.addr = str_addr
	node.id = getHash(str_addr)

	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()

	node.mu.Lock()
	node.pool = make(map[string]*rpc.Client)
	node.mu.Unlock()

	for i := 0; i < m; i++ {
		node.kBucket[i].Init(node.addr)
	}
}

//-------------------
//for debug
func (node *Node) RPCPrintInfo(_ string, _ *struct{}) error {
	if node.online {
		logrus.Infof("[info] 当前节点：[%s] is online", node.addr)
	} else {
		logrus.Infof("[info] 当前节点：[%s] is offline", node.addr)
	}
	return nil
}
func (node *Node) Debug() {
	node.RPCPrintInfo("", nil)
}
func (node *Node) AddInfo(info string) {
	logrus.Info(info)
}

//--------------------
func (node *Node) RunRPCServer() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		logrus.Error("[error] Register error: ", node.addr, err)
		return
	}
	node.listener, err = net.Listen("tcp", node.addr)
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	// logrus.Infoln("[Success] Run: ", node.addr)

	for node.listening {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}
func (node *Node) RemoteCall(addrStr string, method string, args interface{}, reply interface{}) error {
	// logrus.Infof("[RemoteCall] [%s] Call [%s] [%s] [%v] ", node.addr, addrStr, method, args)
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
		logrus.Warnf("[RemoteCall] [%s] when [%s] Call [%s] [%s] [%v] ", err, node.addr, addrStr, method, args)
		node.mu.Lock()
		delete(node.pool, addrStr)
		node.mu.Unlock()
		client.Close()
	}
	return err
}

func (node *Node) NotifyCall(addrStr string, method string, args interface{}, reply interface{}) error {
	logrus.Infof("[NotifyCall]")
	err := node.RemoteCall(addrStr, method, args, reply)
	if err != nil {
		logrus.Errorf("[NotifyCall] [%s] calling %s [%s] occers error [%s]", node.addr, method, addrStr, err)
		return err
	}
	err = node.RemoteCall(addrStr, "Node.RPCNotifyPing", node.addr, nil)
	if err != nil {
		logrus.Errorf("[NotifyCall] [%s] NotifyPing [%s] occers error [%s]", node.addr, addrStr, err)
		return err
	}
	return nil
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
	if addr_str == node.addr {
		return node.online
	}
	err := node.RemoteCall(addr_str, "Node.RPCPing", "", nil)
	if err != nil {
		return false
	} else {
		return true
	}
}
func (node *Node) NotifyPing(addr_str string) bool {
	if addr_str == node.addr {
		return node.online
	}
	err := node.RemoteCall(addr_str, "Node.RPCNotifyPing", node.addr, nil)
	if err != nil {
		return false
	} else {
		return true
	}
}
func (node *Node) RPCPing(_ string, _ *struct{}) error {
	if node.online {
		return nil
	}
	return errors.New("[node closed]")
}
func (node *Node) RPCNotifyPing(caller string, _ *struct{}) error {
	node.UpdateBucket(caller)
	if node.online {
		return nil
	}
	return errors.New("[node closed]")
}

//-----------------
func (node *Node) PutUnit(pair DataPair, _ *struct{}) error {

	return nil
}
func (node *Node) Store(pair DataPair, _ *struct{}) error {
	var strValue string
	retList := node.Lookup(pair.Key)
	for e := retList.L.Front(); e != nil; e = e.Next() {
		strValue, _ = e.Value.(string)
		node.NotifyCall(strValue, "Node.PutUnit", pair, nil)
	}
	return nil
}

//--------------------
// 返回离target最近的k个节点
func (node *Node) FindNode(target_addr string, retSlice *KListSlice) error {
	defer logrus.Infof("[FindNode] end")
	ret := NewListPtr(target_addr)
	// retList.Init(target_addr)
	ind := cpl(getHash(target_addr), getHash(node.addr))
	//现在对应的kBucket中查找
	if ind < 0 {
		//找到目标节点
		ret.UpdateKList(node, target_addr)
	} else {
		node.kBucket[ind].Mu.RLock()
		for e := node.kBucket[ind].L.Front(); e != nil; e = e.Next() {
			strValue, _ := e.Value.(string)
			ret.UpdateKList(node, strValue)
		}
		node.kBucket[ind].Mu.RUnlock()
	}
	if ret.Size() == kSize {
		*retSlice = ret.ToKListSlice()
		logrus.Infof("[FindNode] [%s] find [%s], ret_list: %s", node.addr, target_addr, ret.Print())
		return nil
	}
	for i := ind - 1; i >= 0; i-- {
		node.kBucket[i].Mu.RLock()
		for e := node.kBucket[i].L.Front(); e != nil; e = e.Next() {
			strValue, _ := e.Value.(string)
			ret.UpdateKList(node, strValue)
		}
		node.kBucket[i].Mu.RUnlock()
	}
	ret.UpdateKList(node, node.addr)
	if ret.Size() == kSize {
		*retSlice = ret.ToKListSlice()
		logrus.Infof("[FindNode] [%s] find [%s], ret_list: %s", node.addr, target_addr, ret.Print())
		return nil
	}
	for i := ind + 1; i < m; i++ {
		node.kBucket[i].Mu.RLock()
		for e := node.kBucket[i].L.Front(); e != nil; e = e.Next() {
			strValue, _ := e.Value.(string)
			ret.UpdateKList(node, strValue)
		}
		node.kBucket[i].Mu.RUnlock()
	}
	*retSlice = ret.ToKListSlice()
	logrus.Infof("[FindNode] [%s] find [%s], ret_list: %s", node.addr, target_addr, ret.Print())
	return nil
}
func (node *Node) Lookup(target_addr string) KList {
	//a combination of SPFA and Dijkstra, lol
	var retList KList
	var initListSlice, retListSlice KListSlice
	err := node.FindNode(target_addr, &initListSlice)
	initList := FromKListSlice(initListSlice)

	if err != nil {
		logrus.Errorf("[Lookup] %s fail", node.addr)
	}
	logrus.Infof("[Lookup] [%s] init_list: %s", node.addr, initList.Print())

	visited := make(map[string]bool)
	flag := true

	//todo 加上并行
	for flag {
		var fail_nodes []string
		flag = false
		shortList := NewList(target_addr)
		for e := initList.L.Front(); e != nil; e = e.Next() {
			strValue, _ := e.Value.(string)

			logrus.Infof("[Lookup] [%s] has [%s] in list", node.addr, strValue)

			if visited[strValue] {
				continue
			}
			node.UpdateBucket(strValue)
			visited[strValue] = true

			// retList.Init(target_addr)
			err = node.NotifyCall(strValue, "Node.FindNode", target_addr, &retListSlice)
			retList = FromKListSlice(retListSlice)

			if err != nil {
				logrus.Warnf("[Lookup] [%s] fail finding [%s]", strValue, target_addr)
				fail_nodes = append(fail_nodes, strValue)
			} else {
				//update shortList
				for e_ret := retList.L.Front(); e_ret != nil; e_ret = e_ret.Next() {
					strValue, ok := e.Value.(string)
					if !ok {
						logrus.Errorf("[error] cannot convert e_ret.Value to string")
						continue
					}
					shortList.UpdateKList(node, strValue)
				}
			}
			//delete offline nodes in initList
			for _, val := range fail_nodes {
				initList.Remove(val)
				flag = true
			}
			//update initList by ShortList
			for e_ret := retList.L.Front(); e_ret != nil; e_ret = e_ret.Next() {
				strValue, _ := e.Value.(string)
				initList.UpdateKList(node, strValue)
			}
		}
	}
	return initList
}

func (node *Node) UpdateBucket(addr string) {
	if addr == "" || addr == node.addr || !node.Ping(addr) {
		return
	}
	ind := cpl(getHash(addr), getHash(node.addr))
	node.kBucket[ind].UpdateBucket(node, addr)
	logrus.Infof("update kBucket, [%s]'s %d-Bucket [%s]", node.addr, ind, addr)
}

func (node *Node) Maintain() {
	go func() {
		for node.online {
			// node.Republish()
			time.Sleep(RepublishInterval)
		}
		logrus.Info("[end]Publish end: ", node.addr)
	}()
}

// -----------------
func (node *Node) Create() {
	node.Maintain()
	logrus.Info("创建成功, 开始维护, 初始节点为", node.addr)
}
func (node *Node) Run() {
	node.online = true
	node.listening = true
	go node.RunRPCServer()
}
func (node *Node) Join(introducer string) bool {
	logrus.Infof("%s 加入 %s", node.addr, introducer)
	//该节点已经下线则error
	if !node.Ping(introducer) {
		logrus.Error("[error] Join: provided node is offline, ", node.addr, " ", introducer)
		return false
	}
	node.UpdateBucket(introducer)
	node.Lookup(node.addr)
	node.Maintain()
	return true
}

func (node *Node) Put(key string, value string) bool {
	logrus.Infof("存储 %s %s", key, value)
	err := node.Store(DataPair{key, value}, nil)
	// logrus.Infof("[Put] [%s] %s", node.Addr, key)
	if err != nil {
		logrus.Errorf("[Error] [Put] Put: [%s] <-- (%s , %s) ", node.addr, key, value)
		return false
	}
	// logrus.Infof("[Success] Put: [%s] <-- (%s , %s) ", target_addr, key, value)
	return true
}
func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("获取 %s", key)
	return true, ""
}

func (node *Node) Delete(key string) bool {
	logrus.Infof("删除 %s", key)
	return true
}

func (node *Node) Quit() {
	logrus.Infof("退出 %s", node.addr)
	node.turnOffNode()
}
func (node *Node) ForceQuit() {
	logrus.Infof("%s 强制退出", node.addr)
	node.turnOffNode()
}

func (node *Node) turnOffNode() {
	node.listening = false
	err := node.listener.Close()
	if err != nil {
		logrus.Errorf("[error] Quit [%s], error:[%s]", node.addr, err)
	}
	node.online = false
	logrus.Infof("[Success] Quit: [%s]", node.addr)
}
