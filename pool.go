// Package grpcpool provides a pool of grpc clients
package grpcpool

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// IPool grpc pool behavior set
type IPool interface {
	// Get obtain one *ClientConn
	Get(ctx context.Context) (*ClientConn, error)
	// IsClosed pool is closed or not
	IsClosed() bool
	// Close close the pool
	Close()
	// Capacity
	Capacity() int
	// Capacity
	Available() int
}

// PoolConfig pool param config
type PoolConfig struct {
	Factory         Factory       // build grpc dial
	IdleTimeout     time.Duration // max idle time for per grpc conn
	MaxLifeDuration time.Duration // max ttl for  per grpc conn
	Init            int           // init num
	Capacity        int           // max num
}

var (
	// ErrClosed is the error when the client pool is closed
	ErrClosed = errors.New("grpc pool: client pool is closed")
	// ErrTimeout is the error when the client pool timed out
	ErrTimeout = errors.New("grpc pool: client pool timed out")
	// ErrAlreadyClosed is the error when the client conn was already closed
	ErrAlreadyClosed = errors.New("grpc pool: the connection was already closed")
	// ErrFullPool is the error when the pool is already full
	ErrFullPool = errors.New("grpc pool: closing a ClientConn into a full pool")
)

// Factory is a function type creating a grpc client
type Factory func() (*grpc.ClientConn, error)

// pool is the grpc client pool
type pool struct {
	clients         chan ClientConn
	factory         Factory
	idleTimeout     time.Duration
	maxLifeDuration time.Duration
	mu              sync.RWMutex
}

// ClientConn is the wrapper for a grpc client conn
type ClientConn struct {
	*grpc.ClientConn
	pool          *pool
	timeUsed      time.Time
	timeInitiated time.Time
	unhealthy     bool
}

// New creates a new clients pool with the given initial amd maximum capacity,
// and the timeout for the idle clients. Returns an error if the initial
// clients could not be created
func New(pc PoolConfig) (*pool, error) {

	if pc.Capacity <= 0 {
		pc.Capacity = 1
	}
	if pc.Init < 0 {
		pc.Init = 0
	}
	if pc.Init > pc.Capacity {
		pc.Init = pc.Capacity
	}
	p := &pool{
		clients:     make(chan ClientConn, pc.Capacity),
		factory:     pc.Factory,
		idleTimeout: pc.IdleTimeout,
	}

	for i := 0; i < pc.Init; i++ {
		c, err := pc.Factory()
		if err != nil {
			return nil, err
		}

		p.clients <- ClientConn{
			ClientConn:    c,
			pool:          p,
			timeUsed:      time.Now(),
			timeInitiated: time.Now(),
		}
	}
	// Fill the rest of the pool with empty clients
	for i := 0; i < pc.Capacity-pc.Init; i++ {
		p.clients <- ClientConn{
			pool: p,
		}
	}
	return p, nil
}

func (p *pool) getClients() chan ClientConn {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.clients
}

// Close empties the pool calling Close on all its clients.
// You can call Close while there are outstanding clients.
// It waits for all clients to be returned (Close).
// The pool channel is then closed, and Get will not be allowed anymore
func (p *pool) Close() {
	p.mu.Lock()
	clients := p.clients
	p.clients = nil
	p.mu.Unlock()

	if clients == nil {
		return
	}

	close(clients)
	for i := 0; i < p.Capacity(); i++ {
		client := <-clients
		if client.ClientConn == nil {
			continue
		}
		client.ClientConn.Close()
	}
}

// IsClosed returns true if the client pool is closed.
func (p *pool) IsClosed() bool {
	return p == nil || p.getClients() == nil
}

// Get will return the next available client. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait till the next client becomes available or a timeout.
// A timeout of 0 is an indefinite wait
func (p *pool) Get(ctx context.Context) (*ClientConn, error) {
	clients := p.getClients()
	if clients == nil {
		return nil, ErrClosed
	}

	wrapper := ClientConn{
		pool: p,
	}
	select {
	case wrapper = <-clients:
		// All good
	case <-ctx.Done():
		return nil, ErrTimeout
	}

	// If the wrapper was idle too long, close the connection and create a new
	// one. It's safe to assume that there isn't any newer client as the client
	// we fetched is the first in the channel
	idleTimeout := p.idleTimeout
	if wrapper.ClientConn != nil && idleTimeout > 0 &&
		wrapper.timeUsed.Add(idleTimeout).Before(time.Now()) {

		wrapper.ClientConn.Close()
		wrapper.ClientConn = nil
	}

	var err error
	if wrapper.ClientConn == nil {
		wrapper.ClientConn, err = p.factory()
		if err != nil {
			// If there was an error, we want to put back a placeholder
			// client in the channel
			clients <- ClientConn{
				pool: p,
			}
		}
		// This is a new connection, reset its initiated time
		wrapper.timeInitiated = time.Now()
	}

	return &wrapper, err
}

// Unhealthy marks the client conn as unhealthy, so that the connection
// gets reset when closed
func (c *ClientConn) Unhealthy() {
	c.unhealthy = true
}

// Close returns a ClientConn to the pool. It is safe to call multiple time,
// but will return an error after first time
func (c *ClientConn) Close() error {
	if c == nil {
		return nil
	}
	if c.ClientConn == nil {
		return ErrAlreadyClosed
	}
	if c.pool.IsClosed() {
		return ErrClosed
	}
	// If the wrapper connection has become too old, we want to recycle it. To
	// clarify the logic: if the sum of the initialization time and the max
	// duration is before Now(), it means the initialization is so old adding
	// the maximum duration couldn't put in the future. This sum therefore
	// corresponds to the cut-off point: if it's in the future we still have
	// time, if it's in the past it's too old
	maxDuration := c.pool.maxLifeDuration
	if maxDuration > 0 && c.timeInitiated.Add(maxDuration).Before(time.Now()) {
		c.Unhealthy()
	}

	// We're cloning the wrapper so we can set ClientConn to nil in the one
	// used by the user
	wrapper := ClientConn{
		pool:       c.pool,
		ClientConn: c.ClientConn,
		timeUsed:   time.Now(),
	}
	if c.unhealthy {
		wrapper.ClientConn.Close()
		wrapper.ClientConn = nil
	} else {
		wrapper.timeInitiated = c.timeInitiated
	}
	select {
	case c.pool.clients <- wrapper:
		// All good
	default:
		return ErrFullPool
	}

	c.ClientConn = nil // Mark as closed
	return nil
}

// Capacity returns the capacity
func (p *pool) Capacity() int {
	if p.IsClosed() {
		return 0
	}
	return cap(p.clients)
}

// Available returns the number of currently unused clients
func (p *pool) Available() int {
	if p.IsClosed() {
		return 0
	}
	return len(p.clients)
}
