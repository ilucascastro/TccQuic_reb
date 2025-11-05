package stream_handler

import (
	"log"
	"sync"
	"time"

	"main/src/model"
	"main/src/server/metrics"
)

// ----------------------------- Tipos públicos -----------------------------

type QueuePolicy string

const (
	PolicyFIFO QueuePolicy = "fifo"
	PolicySP   QueuePolicy = "sp"  // strict priority (não-preemptivo)
	PolicyWFQ  QueuePolicy = "wfq" // weighted fair queuing simples
)

// TaskScheduler é a interface usada pelo stream_handler.go
type TaskScheduler interface {
	Enqueue(p model.Priority, fn func()) bool
	Run()
	Stop()
}

// ----------------------------- Implementação -----------------------------

type task struct {
	prio     model.Priority
	fn       func()
	enqueued time.Time
}

type Scheduler struct {
	policy QueuePolicy

	// filas por prioridade (low=0, med=1, high=2)
	queues [int(model.PRIORITY_LEVEL_COUNT)][]task

	// controle de execução
	mu      sync.Mutex
	cond    *sync.Cond
	stopped bool
	running bool

	// WFQ: pesos e estado do RR
	wfqWeights [int(model.PRIORITY_LEVEL_COUNT)]int // default: low=1, med=2, high=3
	wfqCursor  int
	wfqBudget  [int(model.PRIORITY_LEVEL_COUNT)]int
}

// NewTaskScheduler cria um escalonador com a política desejada
func NewTaskScheduler(policy QueuePolicy) TaskScheduler {
	s := &Scheduler{
		policy: policy,
	}
	// pesos WFQ default (ajuste se quiser)
	s.wfqWeights[model.LOW_PRIORITY] = 1
	s.wfqWeights[model.MEDIUM_PRIORITY] = 2
	s.wfqWeights[model.HIGH_PRIORITY] = 3

	s.cond = sync.NewCond(&s.mu)

	// Expor pesos ao módulo de WFQ utilization (se for WFQ)
	if policy == PolicyWFQ {
		metrics.SetWFQWeights(map[int]float64{
			int(model.LOW_PRIORITY):    float64(s.wfqWeights[model.LOW_PRIORITY]),
			int(model.MEDIUM_PRIORITY): float64(s.wfqWeights[model.MEDIUM_PRIORITY]),
			int(model.HIGH_PRIORITY):   float64(s.wfqWeights[model.HIGH_PRIORITY]),
		})
	}

	return s
}

// ----------------------------- API pública -------------------------------

func (s *Scheduler) Enqueue(p model.Priority, fn func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false
	}
	// enfileira
	s.queues[int(p)] = append(s.queues[int(p)], task{
		prio:     p,
		fn:       fn,
		enqueued: time.Now(),
	})

	// backlog mudou (soma de todas as filas)
	metrics.UpdateBacklog(s.totalQueuedLocked())

	// acorda a goroutine do Run
	s.cond.Signal()
	return true
}

func (s *Scheduler) Run() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	log.Printf("[SCHED] running with policy=%s", s.policy)

    for {
        // escolhe próxima tarefa (bloqueando se necessário)
        t, ok := s.nextTaskBlocking()
        if !ok {
            // parado
            break
        }

        // executa fora do lock com proteção contra panics
        func() {
            defer func() {
                if r := recover(); r != nil {
                    log.Printf("[SCHED] task panic recovered: %v", r)
                }
            }()
            if t.fn == nil {
                // tarefa inválida; apenas ignore
                log.Printf("[SCHED] warning: picked task with nil fn")
                return
            }
            t.fn()
        }()
    }
    log.Printf("[SCHED] stopped")
}

func (s *Scheduler) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	s.cond.Broadcast()
}

// ----------------------- QueueLenProvider (opcional) ----------------------

// QueueLenPerClass expõe comprimentos das filas por classe (para sampling)
func (s *Scheduler) QueueLenPerClass() map[model.Priority]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := make(map[model.Priority]int, int(model.PRIORITY_LEVEL_COUNT))
	for i := 0; i < int(model.PRIORITY_LEVEL_COUNT); i++ {
		m[model.Priority(i)] = len(s.queues[i])
	}
	return m
}

// ----------------------------- Seleção ------------------------------------

func (s *Scheduler) nextTaskBlocking() (task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.stopped {
			return task{}, false
		}

		if total := s.totalQueuedLocked(); total > 0 {
			var t task
			switch s.policy {
			case PolicyFIFO:
				t = s.pickFIFO()
			case PolicySP:
				t = s.pickSP()
			case PolicyWFQ:
				t = s.pickWFQ()
			default:
				t = s.pickFIFO()
			}

			// após remover a task das filas, o backlog mudou
			metrics.UpdateBacklog(s.totalQueuedLocked())
			// vamos começar a processar => estado busy
			metrics.UpdateServiceState(true)

			return t, true
		}

		// filas vazias → servidor idle (antes de bloquear)
		metrics.UpdateServiceState(false)

		s.cond.Wait()
	}
}

// total de itens enfileirados (com lock)
func (s *Scheduler) totalQueuedLocked() int {
	n := 0
	for i := 0; i < int(model.PRIORITY_LEVEL_COUNT); i++ {
		n += len(s.queues[i])
	}
	return n
}

// ----------------------------- Políticas ----------------------------------

// FIFO global: pega o mais antigo entre todas as filas.
func (s *Scheduler) pickFIFO() task {
	// encontra o task com menor enqueued
	found := false
	var bestIdx int
	var bestQ int
	var bestEnq time.Time

	for q := 0; q < int(model.PRIORITY_LEVEL_COUNT); q++ {
		if len(s.queues[q]) == 0 {
			continue
		}
		t := s.queues[q][0]
		if !found || t.enqueued.Before(bestEnq) {
			found = true
			bestEnq = t.enqueued
			bestIdx = 0
			bestQ = q
		}
	}
	t := s.queues[bestQ][bestIdx]
	// remove da fila
	s.queues[bestQ] = s.queues[bestQ][1:]
	return t
}

// SP (strict priority, não-preemptivo): sempre escolhe a maior prioridade disponível.
func (s *Scheduler) pickSP() task {
	// high → medium → low
    for q := int(model.HIGH_PRIORITY); q <= int(model.LOW_PRIORITY); q++ {
		if len(s.queues[q]) > 0 {
			t := s.queues[q][0]
			s.queues[q] = s.queues[q][1:]
			// (se implementar preempção real no futuro, chame metrics.M().OnPreempt)
			return t
		}
	}
	// não deveria chegar aqui (já verificamos total > 0)
	return task{}
}

// WFQ simples por fatia de tarefas (peso = nº de tarefas por rodada)
func (s *Scheduler) pickWFQ() task {
	nClasses := int(model.PRIORITY_LEVEL_COUNT)
	// garante orçamento inicial
	for i := 0; i < nClasses; i++ {
		if s.wfqBudget[i] <= 0 {
			s.wfqBudget[i] = s.wfqWeights[i]
		}
	}

	checked := 0
	for checked < nClasses {
		q := s.wfqCursor % nClasses

		if len(s.queues[q]) > 0 && s.wfqBudget[q] > 0 {
			// serve dessa fila e consome orçamento
			t := s.queues[q][0]
			s.queues[q] = s.queues[q][1:]
			s.wfqBudget[q]--
			// fica no mesmo cursor para tentar servir +1 da mesma fila se ainda há orçamento
			return t
		}

		// avança cursor e, se orçamento esgotado, recarrega
		if s.wfqBudget[q] <= 0 {
			s.wfqBudget[q] = s.wfqWeights[q]
		}
		s.wfqCursor = (s.wfqCursor + 1) % nClasses
		checked++
	}

	// fallback: se nada foi escolhido (todas filas vazias no giro), devolve FIFO
	return s.pickFIFO()
}

// ----------------------------- Utilidades ---------------------------------

// (Opcional) Se em algum momento você adicionar preempção real ao SP, chame isto:
func (s *Scheduler) notifyPreemption(preempted, preemptor model.Priority) {
	metrics.M().OnPreempt(preempted, preemptor)
}
