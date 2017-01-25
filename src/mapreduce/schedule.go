package mapreduce

import (
	"fmt"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	var jobDone chan int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
		jobDone = make(chan int,  ntasks)
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
		jobDone = make(chan int, ntasks)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	for i := 0; i < ntasks; i++ {
		go func(i int) {
			for {
				worker := <-mr.registerChannel

				workerArg := &DoTaskArgs{
					File:mr.files[i],
					JobName:mr.jobName,
					NumOtherPhase:nios,
					Phase:phase,
					TaskNumber:i,
				}

				if ok := call(worker, "Worker.DoTask", workerArg, new(struct{})); ok == true {
					go func() {
						mr.registerChannel <- worker
					}()

					jobDone <- i
					//fmt.Printf("Task %d comes into jobDone channel\n", i)
					break
				}
			}
		}(i)
	}

	//wait until all map are done
	for k := 0; k < ntasks; k++ {
		<-jobDone
		//fmt.Printf("Task %d leave jobDone channel\n", temId)
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

//func (mr *Master) process(jobDone chan int, taskId int, nios int, phase jobPhase) {
//	worker := <-mr.registerChannel
//
//	for {
//		workerArg := &DoTaskArgs{}
//
//		workerArg.File = mr.files[taskId]
//		workerArg.JobName = mr.jobName
//		workerArg.NumOtherPhase = nios
//		workerArg.Phase = phase
//		workerArg.TaskNumber = taskId
//
//		if call(worker, "Worker.DoTask", workerArg, new(struct{})) == true {
//			mr.registerChannel <- worker
//			jobDone <- taskId
//
//			fmt.Printf("Task %d comes into jobDone channel\n", taskId)
//
//			return
//		}
//	}
//}
