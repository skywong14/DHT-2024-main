// 这个包实现了一个简单的DHT协议。（实际上，它并不是分布式的。）
// 这个协议的性能和可扩展性都很糟糕。
// 你可以参考这个协议来实现其他协议。
//
// 在这个简单的协议中，网络是一个完全图，每个节点存储所有的键值对。
// 当一个节点加入网络时，它会从另一个节点复制所有的键值对。
// 对键值对的任何修改都会广播到所有节点。
// 如果任何RPC调用失败，我们简单地假设目标节点离线并将其从对等列表中移除。
package naive2

import (
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// 注意：init()函数会在包被导入时执行。
// 详情请见https://golang.org/doc/effective_go.html#init。
func init() {
	// 你可以使用logrus包来打印漂亮的日志。
	// 这里我们将日志输出设置到一个文件。
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

// Node表示一个节点结构体
type Node struct {
	Addr   string // 节点的地址和端口号，例如："localhost:1234"
	online bool

	listener  net.Listener
	server    *rpc.Server
	data      map[string]string
	dataLock  sync.RWMutex
	peers     map[string]struct{} // 使用map来表示集合
	peersLock sync.RWMutex
}

// Pair用来存储键值对。
// 注意：它必须导出（即首字母大写），以便可以作为RPC方法的参数类型。
type Pair struct {
	Key   string
	Value string
}

// 初始化节点。
// Addr是节点的地址和端口号，例如："localhost:1234"。
func (node *Node) Init(addr string) {
	node.Addr = addr
	node.data = make(map[string]string)
	node.peers = make(map[string]struct{})
}

// 启动RPC服务器
func (node *Node) RunRPCServer() {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	if err != nil {
		logrus.Fatal("监听错误：", err)
	}
	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("接受错误：", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

// 停止RPC服务器
func (node *Node) StopRPCServer() {
	node.online = false
	node.listener.Close()
}

// 远程调用指定地址的RPC方法。
//
// 注意：空接口可以容纳任何类型的值。(https://tour.golang.org/methods/14)
// 每次重新连接客户端可能会很慢。你可以使用连接池来提高性能。
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Node.Ping" {
		logrus.Infof("[%s] 远程调用 %s %s %v", node.Addr, addr, method, args)
	}
	// 注意：这里我们使用DialTimeout设置10秒的超时。
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		logrus.Error("拨号错误：", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("远程调用错误：", err)
		return err
	}
	return nil
}

// RPC方法
//
// 注意：用于RPC的方法必须导出（即首字母大写），
// 并且必须有两个参数，两个参数都必须是导出（或内置）类型。
// 第二个参数必须是指针。
// 返回类型必须是error。
// 简而言之，方法的签名必须是：
//   func (t *T) MethodName(argType T1, replyType *T2) error
// 详情请见https://golang.org/pkg/net/rpc/。

// 获取数据
func (node *Node) GetData(_ string, reply *map[string]string) error {
	node.dataLock.RLock()
	*reply = node.data
	node.dataLock.RUnlock()
	return nil
}

// 获取对等节点列表
func (node *Node) GetPeers(_ string, reply *map[string]struct{}) error {
	node.peersLock.RLock()
	*reply = node.peers
	node.peersLock.RUnlock()
	return nil
}

// 添加一个新的对等节点地址到当前节点的对等节点列表中。
func (node *Node) AddPeer(addr string, _ *struct{}) error {
	node.peersLock.Lock()
	node.peers[addr] = struct{}{}
	node.peersLock.Unlock()
	return nil
}

// Ping方法 检查当前节点是否在线。
func (node *Node) Ping(_ string, _ *struct{}) error {
	return nil
}

// 从当前节点的对等节点列表中移除一个节点地址。
func (node *Node) RemovePeer(addr string, _ *struct{}) error {
	_, ok := node.peers[addr]
	if !ok {
		return nil
	}
	node.peersLock.Lock()
	delete(node.peers, addr)
	node.peersLock.Unlock()
	return nil
}

// 在当前节点存储一个新的键值对。
func (node *Node) PutPair(pair Pair, _ *struct{}) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	return nil
}

//从当前节点中删除一个键值对。
func (node *Node) DeletePair(key string, _ *struct{}) error {
	node.dataLock.Lock()
	delete(node.data, key)
	node.dataLock.Unlock()
	return nil
}

// DHT方法
// 启动节点
func (node *Node) Run() {
	node.online = true
	go node.RunRPCServer()
}

// 创建节点 初始化并创建一个新的 DHT 网络。当前实现只是打印一条日志
func (node *Node) Create() {
	logrus.Info("创建")
}

// 广播RPC调用到网络中的所有节点。
// 如果某个节点离线，将其从对等列表中移除。
func (node *Node) broadcastCall(method string, args interface{}, reply interface{}) {
	offlinePeers := make([]string, 0)
	node.peersLock.RLock()
	for peer := range node.peers {
		err := node.RemoteCall(peer, method, args, reply)
		if err != nil {
			offlinePeers = append(offlinePeers, peer)
		}
	}
	node.peersLock.RUnlock()
	if len(offlinePeers) > 0 {
		node.peersLock.Lock()
		for _, peer := range offlinePeers {
			delete(node.peers, peer)
		}
		node.peersLock.Unlock()
	}
}

// 加入一个现有的 DHT 网络。从指定节点复制数据和对等节点列表，并通知所有节点新的节点加入。
func (node *Node) Join(addr string) bool {
	logrus.Infof("加入 %s", addr)
	// 从指定地址的节点复制数据。
	node.dataLock.Lock()
	node.RemoteCall(addr, "Node.GetData", "", &node.data)
	node.dataLock.Unlock()
	// 从指定地址的节点复制对等节点列表。
	node.peersLock.Lock()
	node.RemoteCall(addr, "Node.GetPeers", "", &node.peers)
	node.peers[addr] = struct{}{}
	node.peersLock.Unlock()
	// 通知网络中的所有节点有新节点加入。
	node.broadcastCall("Node.AddPeer", node.Addr, nil)
	return true
}

// 在当前节点存储一个新的键值对，并广播给所有对等节点。
func (node *Node) Put(key string, value string) bool {
	logrus.Infof("存储 %s %s", key, value)
	node.dataLock.Lock()
	node.data[key] = value
	node.dataLock.Unlock()
	// 将新的键值对广播到网络中的所有节点。
	node.broadcastCall("Node.PutPair", Pair{key, value}, nil)
	return true
}

// 从当前节点获取一个键的值。
func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("获取 %s", key)
	node.dataLock.RLock()
	value, ok := node.data[key]
	node.dataLock.RUnlock()
	return ok, value
}

// 从当前节点删除一个键值对，并广播给所有对等节点。
func (node *Node) Delete(key string) bool {
	logrus.Infof("删除 %s", key)
	// 检查键是否存在。
	node.dataLock.RLock()
	_, ok := node.data[key]
	node.dataLock.RUnlock()
	if !ok {
		return false
	}
	// 删除键值对。
	node.dataLock.Lock()
	delete(node.data, key)
	node.dataLock.Unlock()
	// 将删除操作广播到网络中的所有节点。
	node.broadcastCall("Node.DeletePair", key, nil)
	return true
}

// 退出网络，并通知所有对等节点将自己从对等节点列表中移除。
func (node *Node) Quit() {
	logrus.Infof("退出 %s", node.Addr)
	// 通知网络中的所有节点此节点正在退出。
	node.broadcastCall("Node.RemovePeer", node.Addr, nil)
	node.StopRPCServer()
}

// 强制退出网络，直接停止 RPC 服务器。
func (node *Node) ForceQuit() {
	logrus.Info("强制退出")
	node.StopRPCServer()
}
