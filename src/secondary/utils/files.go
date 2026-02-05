package utils

import (
	"io"
	"os"
)

func CopyFile(dstName, srcName string) (written int64, err error) {
	// 打开源文件
	src, err := os.Open(srcName)
	if err != nil {
		return 0, err
	}
	defer src.Close()

	// 创建目标文件
	dst, err := os.Create(dstName)
	if err != nil {
		return 0, err
	}
	defer dst.Close()

	// 复制文件内容
	return io.Copy(dst, src)
}
