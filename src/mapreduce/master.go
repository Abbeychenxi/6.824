package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	waitingChannal := make(chan string)
	jobsChannal := make(chan *DoJobArgs)

	workerDoJob := func(addr string, args *DoJobArgs) {
		var reply DoJobReply
		ok := call(addr, "Worker.DoJob", args, &reply)
		if !ok {
			jobsChannal <- args
		} else {
			waitingChannal <- addr
		}
	}

	getNextWorkerAddr := func() string {
		var addr string
		select {
		case addr = <- mr.registerChannel:
			mr.Workers[addr] = &WorkerInfo{addr}
		case addr = <- waitingChannal:
		}
		return addr
	}

	go func() {
		for job := range jobsChannal {
			addr := getNextWorkerAddr()
			go workerDoJob(addr, job)
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
		jobsChannal <- args
	}

	for i := 0; i < mr.nReduce; i++ {
		args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
		jobsChannal <- args
	}

	return mr.KillWorkers()
}
