package pbservice

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongServer      = "ErrWrongServer"
	ErrDuplicateRequest = "ErrDuplicateRequest"
	ErrBackupSyncFail   = "ErrBackupSyncFail"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op string
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64 // for detecting duplicate requests
}

type ForwardedDataArgs struct {
	Data map[string]string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
