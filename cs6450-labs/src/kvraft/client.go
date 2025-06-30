package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	client_Id int64
	request_Id int
	prevLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.client_Id = nrand()
	ck.request_Id = 0
	ck.prevLeader = 0
	return ck
}

func (ck *Clerk) getRequest_Id() int {
	nextReqId := ck.request_Id
	ck.request_Id++
	return nextReqId
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	
	args := GetArgs{
	    Key:        key,
	    Client_Id:  ck.client_Id,
	    Request_Id: ck.getRequest_Id(),
	}
	i := ck.prevLeader

	for {
		reply := GetReply{}
		if ck.servers[i].Call("KVServer.Get", &args, &reply) && reply.Err == OK {
			// Successfully fetched the value.
			ck.prevLeader = i
			return reply.Value
		}

		// Move to the next server in the list.
		i = (i + 1) % len(ck.servers)

		// After one full round, wait briefly before retrying.
		if i == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:        key,
		Value:      value,
		Op:         op,
		Client_Id:  ck.client_Id,
		Request_Id: ck.getRequest_Id(),
	}
	i := ck.prevLeader

	for {
		reply := PutAppendReply{}

		// Make the RPC call and check the result.
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			ck.prevLeader = i
			return
		}

		// Move to the next server.
		i = (i + 1) % len(ck.servers)

		// Wait after cycling through all servers.
		if i == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}