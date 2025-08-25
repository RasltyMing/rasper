package utils

import (
	"testing"
)

func TestRenameFilesByRegex(t *testing.T) {
	err := RenameFilesByRegex("D:\\Temporary\\test\\*.xml",
		"(.+)_(.+).xml",
		"$1.xml")
	if err != nil {
		return
	}
}
