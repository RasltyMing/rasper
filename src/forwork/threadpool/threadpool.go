package main

import (
	"fmt"
	"raselper/src/secondary/utils"
	"time"
)

func main() {
	// 创建线程池，3个worker，任务队列大小100
	pool := utils.NewWorkerPool(3, 100)
	defer pool.Close()

	// 提交任务
	for i := 0; i < 10; i++ {
		taskID := i
		pool.Submit(func() {
			time.Sleep(1000 * time.Millisecond)
			fmt.Printf("Task %d executed by worker\n", taskID)
		})
	}

	// 等待所有任务完成
	pool.Wait()
	fmt.Println("All tasks completed")
}
