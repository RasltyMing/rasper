package main

import (
	"github.com/go-vgo/robotgo"
	"raselper/app/automation/src"
)

func main() {

	Idle := src.NewStatus("Idle")
	Run := src.NewStatus("Run")
	src.RegisterStatus(Idle, func() {
		x, y := robotgo.Location()
		println("Idle: ", x, y)
	})
	src.RegisterStatus(Run, func() {
		x, y := robotgo.Location()
		println("Run: ", x, y)
	})

	src.RegisterTrans(Idle, func() *src.StatusNode {
		x, _ := robotgo.Location()
		if x > 1000 {
			return &Run
		}
		return nil
	})
	src.RegisterTrans(Run, func() *src.StatusNode {
		x, _ := robotgo.Location()
		if x <= 1000 {
			return &Idle
		}
		return nil
	})

	src.Start(Idle)
}

func getLocation() {
	for {
		x, y := robotgo.Location()
		println(x, y)
	}
}
