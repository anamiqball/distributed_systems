package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"bytes"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Client_Id  int64
	Request_Id int
	Opr       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    kv_Store    map[string]string
	dup_Table   map[int64]Op
	lastApplied int
	responseCh  map[int]chan Op
}

func (kv *KVServer) getDupEntry(client_Id int64, request_Id int, key string) (bool, string) {
	if op, exists := kv.dup_Table[client_Id]; exists && op.Request_Id >= request_Id {
		return true, op.Value
	}
	return false, ""
}

func (kv *KVServer) lastOp(client int64) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if op, exists := kv.dup_Table[client]; exists {
		return op.Request_Id
	}
	return -1
}

func (kv *KVServer) isChannel(index int) chan Op {
	if _, exists := kv.responseCh[index]; !exists {
		kv.responseCh[index] = make(chan Op, 1)
	}
	return kv.responseCh[index]
}

func (kv *KVServer) equiOps(a Op, b Op) bool {
	return a.Client_Id == b.Client_Id &&
		a.Request_Id == b.Request_Id &&
		a.Key == b.Key &&
		a.Opr == b.Opr
}



func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	if kv.lastOp(args.Client_Id) >= args.Request_Id {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.kv_Store[args.Key]
		reply.Err = OK
		DPrintf("[%d] GET_DUP client [%d] request_id [%d] index []", kv.me, args.Client_Id, args.Request_Id)
		return
	}

	command := Op{
		Key:        args.Key,
		Value:      "",
		Opr:        "Get",
		Client_Id:  args.Client_Id,
		Request_Id: args.Request_Id,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Retrieve the response channel for this index
	kv.mu.Lock()
	ch := kv.isChannel(index)
	kv.mu.Unlock()

	DPrintf("[%d] GET client [%d] request_id [%d] index [%d]", kv.me, args.Client_Id, args.Request_Id, index)

	// Wait for the operation to complete or time out
	select {
	case op := <-ch:
		if kv.equiOps(op, command) {
			reply.Value = op.Value
			reply.Err = OK
			DPrintf("[%d] GET_COMPLETED client [%d] request_id [%d] index [%d]", kv.me, args.Client_Id, args.Request_Id, index)
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrWrongLeader
		DPrintf("[%d] TIMEOUT_GET client [%d] request_id [%d] index [%d]", kv.me, args.Client_Id, args.Request_Id, index)
	}
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.lastOp(args.Client_Id) >= args.Request_Id {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Err = OK
		DPrintf("[%d] PUT_DUP client [%d] request_id [%d] index []", kv.me, args.Client_Id, args.Request_Id)
		return
	}

	// Create the command for the Put or Append operation
	command := Op{
		Key:        args.Key,
		Value:      args.Value,
		Opr:        args.Op,
		Client_Id:  args.Client_Id,
		Request_Id: args.Request_Id,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.isChannel(index)
	kv.mu.Unlock()

	DPrintf("[%d] PUT client [%d] request_id [%d] index [%d]", kv.me, args.Client_Id, args.Request_Id, index)

	select {
	case op := <-ch:
		if kv.equiOps(op, command) {
			reply.Err = OK
			DPrintf("[%d] PUT_COMPLETED client [%d] request_id [%d] index [%d]", kv.me, args.Client_Id, args.Request_Id, index)
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(300 * time.Millisecond):
		DPrintf("[%d] TIMEOUT_PUT client [%d] request_id [%d] index [%d]", kv.me, args.Client_Id, args.Request_Id, index)
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) callSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if snapshotting is necessary based on state size
	if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)

		if err := e.Encode(kv.kv_Store); err != nil {
			DPrintf("Error encoding kv_Store")
		}
		if err := e.Encode(kv.dup_Table); err != nil {
			DPrintf("Error encoding dup_Table")
		}

		kvState := w.Bytes()

		kv.rf.Snapshot(kv.lastApplied, kvState)
		DPrintf("[%d] SNAPSHOT client [] request_id [] index [%d]", kv.me, kv.lastApplied)
	}
}

func (kv *KVServer) restoreState(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var tmpKVStore map[string]string
	var tmpDupTable map[int64]Op

	if d.Decode(&tmpKVStore) != nil || d.Decode(&tmpDupTable) != nil {
		DPrintf("Error in restoring state!")
	} else {
		// Successfully restored state; update the server's data
		kv.kv_Store = tmpKVStore
		kv.dup_Table = tmpDupTable
	}
}

func (kv *KVServer) receiveUpdates() {
	for response := range kv.applyCh {
		DPrintf("[%d] RECEIVED client [] request_id [] index [%d]", kv.me, response.CommandIndex)

		if response.CommandValid {
			cmd := response.Command.(Op)
			index := response.CommandIndex
			k, v := cmd.Key, cmd.Value
			op := cmd.Opr
			clientId, requestId := cmd.Client_Id, cmd.Request_Id

			DPrintf("[%d] RESPONSE client [%d] request_id [%d] index [%d]", kv.me, clientId, requestId, index)

			kv.mu.Lock()
			if kv.lastApplied >= index {
				kv.mu.Unlock()
				continue
			}

			// Handle duplicate requests or apply new operations
			ch := kv.isChannel(index)
			if ok, val := kv.getDupEntry(clientId, requestId, k); ok {
				cmd.Value = val
			} else {
				switch op {
				case "Get":
					cmd.Value = kv.kv_Store[k]
					kv.dup_Table[clientId] = cmd
				case "Append":
					kv.kv_Store[k] += v
					cmd.Value = kv.kv_Store[k]
					kv.dup_Table[clientId] = cmd
				case "Put":
					kv.kv_Store[k] = v
					cmd.Value = kv.kv_Store[k]
					kv.dup_Table[clientId] = cmd
				}
			}

			kv.lastApplied = index
			DPrintf("[%d] APPLIED client [%d] request_id [%d] index [%d]", kv.me, clientId, requestId, index)
			kv.mu.Unlock()

			ch <- cmd
		}

		if response.SnapshotValid {
			kv.restoreState(response.Snapshot)
			kv.mu.Lock()
			kv.lastApplied = response.SnapshotIndex
			kv.mu.Unlock()
		}

		kv.callSnapshot()
	}
}


// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
		kv.kv_Store = make(map[string]string)
	kv.responseCh = make(map[int]chan Op)
	kv.dup_Table = make(map[int64]Op)
	kv.lastApplied = 0

	kv.restoreState(persister.ReadSnapshot())

	go kv.receiveUpdates()

	return kv
}