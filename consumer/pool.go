package consumer

import (
	"log"
	"sync"

	"github.com/jbub/rabbitmq-cli-consumer/domain"
)

type worker struct {
	index      int
	workerPool chan *worker
	jobChannel chan domain.Job
	stop       chan struct{}
	infLogger  *log.Logger
	errLogger  *log.Logger
}

func (w *worker) start() {
	go func() {
		var job domain.Job
		for {
			w.workerPool <- w

			select {
			case job = <-w.jobChannel:
				job.Do(w.index, w.infLogger, w.errLogger)
			case <-w.stop:
				w.stop <- struct{}{}
				return
			}
		}
	}()
}

func newWorker(index int, pool chan *worker, infLogger *log.Logger, errLogger *log.Logger) *worker {
	return &worker{
		index:      index,
		workerPool: pool,
		jobChannel: make(chan domain.Job),
		stop:       make(chan struct{}),
		infLogger:  infLogger,
		errLogger:  errLogger,
	}
}

type dispatcher struct {
	workerPool chan *worker
	jobQueue   chan domain.Job
	stop       chan struct{}
}

func (d *dispatcher) dispatch() {
	for {
		select {
		case job := <-d.jobQueue:
			worker := <-d.workerPool
			worker.jobChannel <- job
		case <-d.stop:
			for i := 0; i < cap(d.workerPool); i++ {
				worker := <-d.workerPool

				worker.stop <- struct{}{}
				<-worker.stop
			}

			d.stop <- struct{}{}
			return
		}
	}
}

func newDispatcher(workerPool chan *worker, jobQueue chan domain.Job, infLogger *log.Logger, errLogger *log.Logger) *dispatcher {
	d := &dispatcher{
		workerPool: workerPool,
		jobQueue:   jobQueue,
		stop:       make(chan struct{}),
	}

	for i := 0; i < cap(d.workerPool); i++ {
		worker := newWorker(i, d.workerPool, infLogger, errLogger)
		worker.start()
	}

	go d.dispatch()
	return d
}

type Pool struct {
	JobQueue   chan domain.Job
	dispatcher *dispatcher
	wg         sync.WaitGroup
}

func NewPool(numWorkers int, jobQueueLen int, infLogger *log.Logger, errLogger *log.Logger) *Pool {
	jobQueue := make(chan domain.Job, jobQueueLen)
	workerPool := make(chan *worker, numWorkers)
	return &Pool{
		JobQueue:   jobQueue,
		dispatcher: newDispatcher(workerPool, jobQueue, infLogger, errLogger),
	}
}

func (p *Pool) AddJob(job domain.Job) {
	p.JobQueue <- job
}

func (p *Pool) JobDone() {
	p.wg.Done()
}

func (p *Pool) WaitCount(count int) {
	p.wg.Add(count)
}

func (p *Pool) WaitAll() {
	p.wg.Wait()
}

func (p *Pool) Release() {
	p.dispatcher.stop <- struct{}{}
	<-p.dispatcher.stop
}
