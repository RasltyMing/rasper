package utils

import "sync"

// WorkerPool 基础线程池结构
type WorkerPool struct {
	workerCount int
	taskQueue   chan func()
	wg          sync.WaitGroup
}

// NewWorkerPool 创建新的线程池
func NewWorkerPool(workerCount, queueSize int) *WorkerPool {
	pool := &WorkerPool{
		workerCount: workerCount,
		taskQueue:   make(chan func(), queueSize),
	}

	// 启动worker
	for i := 0; i < workerCount; i++ {
		go pool.worker(i)
	}

	return pool
}

func (p *WorkerPool) worker(id int) {
	for task := range p.taskQueue {
		task()
		p.wg.Done()
	}
}

// Submit 提交任务
func (p *WorkerPool) Submit(task func()) {
	p.wg.Add(1)
	p.taskQueue <- task
}

// Wait 等待所有任务完成
func (p *WorkerPool) Wait() {
	p.wg.Wait()
}

// Close 关闭线程池
func (p *WorkerPool) Close() {
	close(p.taskQueue)
}
