package main

import (
	"fmt"
	"time"
)

func main() {
	str := "17013" + fmt.Sprintf("%d", time.Now().UnixMilli())
	fmt.Println(str)
}
