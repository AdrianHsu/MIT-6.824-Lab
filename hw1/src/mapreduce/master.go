package mapreduce

import (
	"container/list"
)
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

func setupWorkers(mr *MapReduce) {
	for wkr := range mr.registerChannel {
		mr.Workers[wkr] = &WorkerInfo{address: wkr}
		mr.availableWorkers <- wkr
	}
}

func allocate(mr *MapReduce) {
	for i := 0; i < mr.nMap; i++ {
		mr.remainMapJobs <- i
	}
	for i := 0; i < mr.nReduce; i++ {
		mr.remainReduceJobs <- i
	}
}

func doMap(mr *MapReduce) {
	for job := range mr.remainMapJobs { // keep listening
		wkr := <-mr.availableWorkers
		go func(job int, wkr string) {

			args := &DoJobArgs{File: mr.file, Operation: Map,
				JobNumber: job, NumOtherPhase: mr.nReduce}

			var reply DoJobReply
			ok := call(wkr, "Worker.DoJob", args, &reply)
			if ok == false {
				fmt.Printf("DoMap: RPC %s do job failure! reassign the job...\n", wkr)
				mr.remainMapJobs <- job
			} else {
				mr.availableWorkers <- wkr
				mr.nCount <- true
			}
		}(job, wkr)
	}
}


func doReduce(mr *MapReduce) {
	for job := range mr.remainReduceJobs { // keep listening
		wkr := <-mr.availableWorkers
		go func(job int, wkr string) {

			args := &DoJobArgs{File: mr.file, Operation: Reduce,
				JobNumber: job, NumOtherPhase: mr.nMap}

			var reply DoJobReply
			ok := call(wkr, "Worker.DoJob", args, &reply)
			if ok == false {
				fmt.Printf("DoReduce: RPC %s do job failure! reassign the job...\n", wkr)
				mr.remainReduceJobs <- job
			} else {
				mr.availableWorkers <- wkr
				mr.nCount <- true
			}
		}(job, wkr)
	}
}

func mapCountFinishJobs(mr *MapReduce) {
	cnt := 0
	for range mr.nCount {
		cnt += 1
		if cnt == mr.nMap {
			break
		}
	}
	close(mr.remainMapJobs)
	mr.donePhase <- true
}

func reduceCountFinishJobs(mr *MapReduce) {
	cnt := 0
	for range mr.nCount {
		cnt += 1
		if cnt == mr.nReduce {
			break
		}
	}
	close(mr.remainReduceJobs)
	mr.donePhase <- true
}


func (mr *MapReduce) RunMaster() *list.List {
	// Your code here


	go setupWorkers(mr)
	go allocate(mr)

	// Map Phase
	go mapCountFinishJobs(mr)
	doMap(mr)
	<-mr.donePhase
	// Reduce Phase
	go reduceCountFinishJobs(mr)
	doReduce(mr)
	<-mr.donePhase
	return mr.KillWorkers()
}
