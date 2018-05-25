package metric

import (
	"time"
	"sync"
	"sync/atomic"

	"golang.org/x/net/context"
)

const (
	METRIC_SAMPLES = 16
)

type Metric struct {
	lock sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
	metric map[int]*metric // type
}

type metric struct {
	currentCount uint64

	lastSampleTime int
	lastSampleCount uint64
	samples [METRIC_SAMPLES]uint64
	idx int
}

func New() *Metric {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Metric{
		ctx: ctx,
		cancel: cancel,
		metric: make(map[int]*metric),
	}

	go func() {
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-m.ctx.Done():
				return
			case <-timer.C:
				m.track()
			}
		}
	}()

	return m
}

func (m *Metric) Close() {
	m.cancel()
}

func (m *Metric) findMetric(type_ int) (*metric, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if _metric, ok := m.metric[type_]; ok {
		return _metric, true
	}
	return nil, false
}

func (m *Metric) Stats(type_ int, currentReading int) {
	if currentReading < 0 {
		return
	}

	_metric, find := m.findMetric(type_)
	if !find {
		var ok  bool
		m.lock.Lock()
		if _metric, ok = m.metric[type_]; !ok {
			_metric = &metric{
				lastSampleTime: time.Now().Second(),
			}
			m.metric[type_] = _metric
		}
		m.lock.Unlock()
	}
	atomic.AddUint64(&_metric.currentCount, uint64(currentReading))
}

func (m *Metric) track() {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, mtrc := range m.metric {
		currentCount := mtrc.currentCount
		lastCount := mtrc.lastSampleCount
		lastTime := mtrc.lastSampleTime

		now := time.Now().Second()
		t := now - lastTime
		var ops uint64
		ops = currentCount - lastCount

		var opsSec uint64
		if t > 0 {
			opsSec = ops/uint64(t)
		} else {
			opsSec = 0
		}

		mtrc.samples[mtrc.idx] = opsSec
		mtrc.idx++
		mtrc.idx %= METRIC_SAMPLES
		mtrc.lastSampleCount = currentCount
		mtrc.lastSampleTime = now
	}
}

func (m *Metric) Track(type_ int) uint64 {
	metric, find := m.findMetric(type_)
	if !find {
		return 0
	}

	var sum uint64 = 0
	for j := 0; j < METRIC_SAMPLES; j++ {
		sum += metric.samples[j]
	}
	return sum / METRIC_SAMPLES
}

