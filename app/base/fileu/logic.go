package fileu

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
)

func CopyFile(sourcePath string, targetPath string, config *Config) error {
	if sourcePath == targetPath {
		return errors.New("复制路径相同")
	}

	input, err := os.Open(sourcePath)
	reader := bufio.NewReader(input)
	_ = os.MkdirAll(filepath.Dir(targetPath), os.ModePerm)
	output, err := os.Create(targetPath)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}
	for {
		line, _ := reader.ReadBytes('\n')
		_, err := output.Write(line)
		if err != nil {
			return err
		}
		if len(line) == 0 {
			break
		}
	}

	if err != nil {
		return err
	}

	err = input.Close()
	err = output.Close()
	if err != nil {
		return err
	}

	println("复制:", sourcePath, " 到:", targetPath)

	return nil
}

//func InsertText(filePath string, from string, insert string) error {
//	tmpPath := filePath + ".tmp"
//	err := CopyFile(filePath, tmpPath, func(bytes []byte) []byte {
//		line := string(bytes)
//		if strings.Contains(line, from) {
//			line = strings.Replace(line, from, from+insert, -1)
//		}
//		return []byte(line)
//	})
//	if err != nil {
//		return err
//	}
//	err = os.Remove(filePath)
//	if err != nil {
//		return err
//	}
//	err = os.Rename(tmpPath, filePath)
//	if err != nil {
//		return err
//	}
//	return nil
//}
