package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
		mapDone := make(chan int,  ntasks)

		for i := 0; i < ntasks; i++ {
			go mr.processMap(mapDone, i)
		}

		//wait until all map are done
		for i := 0; i < ntasks; i++ {
			<-mapDone
		}
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
		reduceDone := make(chan int, mr.nReduce)

		for i := 0; i < ntasks; i++ {
			go mr.processReduce(reduceDone, i)
		}

		//wait until all map are done
		for i := 0; i < ntasks; i++ {
			<-reduceDone
		}
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master) processMap(mapDone chan int, mapTaskId int) {
	for {
		worker := <-mr.registerChannel

		workerArg := &DoTaskArgs{}

		workerArg.File = mr.files[mapTaskId]
		workerArg.JobName = mr.jobName
		workerArg.NumOtherPhase = mr.nReduce
		workerArg.Phase = "Map"
		workerArg.TaskNumber = mapTaskId

		if call(worker, "Worker.DoTask", workerArg, nil) == true {
			mr.registerChannel <- worker
			mapDone <- mapTaskId

			return
		}
	}
}

func (mr *Master) processReduce(reduceDone chan int, reduceTaskId int) {
	for {
		worker := <-mr.registerChannel

		workerArg := &DoTaskArgs{}

		workerArg.File = mr.files[reduceTaskId]
		workerArg.JobName = mr.jobName
		workerArg.NumOtherPhase = len(mr.files)
		workerArg.Phase = "Reduce"
		workerArg.TaskNumber = reduceTaskId

		if call(worker, "Worker.DoTask", workerArg, nil) == true {
			mr.registerChannel <- worker
			reduceDone <- reduceTaskId

			return
		}
	}
}