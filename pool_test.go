package grpcpool

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
)

const (
	_test_init     = 1
	_test_capacity = 2
	address        = "localhost:50551"
)

func TestNew(t *testing.T) {
	factory := func() (*grpc.ClientConn, error) {
		return grpc.Dial(address, grpc.WithInsecure())
	}

	pc := PoolConfig{
		Factory:  factory,
		Init:     _test_init,
		Capacity: _test_capacity,
	}
	p, err := New(pc)
	Convey("new pool", t, func() {
		So(err, ShouldEqual, nil)
		Convey("get capacity", func() {
			cap := p.Capacity()
			Convey("The value should be equal two", func() {
				So(cap, ShouldEqual, 2)
			})
		})
		Convey("get available", func() {
			avi := p.Available()
			Convey("The value should be equal two", func() {
				So(avi, ShouldEqual, 2)
			})
		})
	})
	c, err := p.Get(context.Background())
	Convey("close client conn", t, func() {
		Convey("get available before close", func() {
			avi := p.Available()
			Convey("The value should be equal one", func() {
				So(avi, ShouldEqual, 1)
			})
		})
		c.Close()
		Convey("get available after close", func() {
			avi := p.Available()
			Convey("The value should be equal two", func() {
				So(avi, ShouldEqual, 2)
			})
		})
	})
}
