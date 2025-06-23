package main

import (
	"reflect"
	"testing"
)

func Test_loadConfigArgs(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := loadConfigArgs(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadConfigArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_GlobFiles(t *testing.T) {
	//files, _ := fileu.GlobFiles("/*")
	//for _, file := range files {
	//	println(file)
	//}
}
