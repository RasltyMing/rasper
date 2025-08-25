package utils

import (
	"archive/zip"
	"bytes"
	"fmt"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func Zip(targetPath string, sourcePath string) error {
	archive, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer archive.Close()

	zipWriter := zip.NewWriter(archive)
	defer zipWriter.Close()

	isEmpty := true // zip文件是否为空?

	err = filepath.Walk(sourcePath, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() || path == targetPath { // 目录跳过, 目标路径跳过
			return nil
		}

		// zip文件
		fmt.Println("zip fileu ", path, " to ", targetPath)
		isEmpty = false
		w, err := zipWriter.Create(filepath.Base(path))
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		if _, err = io.Copy(w, f); err != nil {
			return err
		}

		return nil
	})

	if isEmpty {
		if err := os.Remove(targetPath); err != nil {
			fmt.Println("err: ", err)
		}
	}

	return nil
}

// Unzip 解压zip文件到指定目录
// src: 源zip文件路径
// dest: 目标解压目录
func Unzip(src string, dest string, decode string, mode string) error {
	matches, err := filepath.Glob(src)
	if err != nil {
		return fmt.Errorf("invalid path pattern: %v", err)
	}

	for _, match := range matches {
		fmt.Println("Unzip file:" + match)
		r, err := zip.OpenReader(match)
		if err != nil {
			if mode == "pass" {
				fmt.Println(err)
				continue
			}
			return err
		}
		defer r.Close()

		// 确保目标目录存在
		err = os.MkdirAll(dest, 0755)
		if err != nil {
			return err
		}

		// 遍历zip文件中的每个文件/文件夹
		for _, f := range r.File {
			fName := f.Name
			// 验证文件路径，防止路径遍历攻击
			if decode == "gbk" {
				reader := bytes.NewReader([]byte(f.Name))
				decoder := transform.NewReader(reader, simplifiedchinese.GB18030.NewDecoder())
				content, _ := ioutil.ReadAll(decoder)
				fName = string(content)
			}
			filePath := filepath.Join(filepath.Dir(match), dest, fName)
			if strings.HasPrefix(dest, "/") {
				filePath = filepath.Join(dest, fName)
			}

			if f.FileInfo().IsDir() {
				// 创建目录
				err = os.MkdirAll(filePath, f.Mode())
				if err != nil {
					return err
				}
			} else {
				// 创建文件
				err = os.MkdirAll(filepath.Dir(filePath), 0755)
				if err != nil {
					return err
				}

				// 打开源文件
				rc, err := f.Open()
				if err != nil {
					return err
				}

				// 创建目标文件
				dstFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
				if err != nil {
					rc.Close()
					return err
				}

				// 复制内容
				_, err = io.Copy(dstFile, rc)
				log.Println("unzip file to ", filePath)
				rc.Close()
				dstFile.Close()
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func Delete(path string) error {
	matches, err := filepath.Glob(path)
	if err != nil {
		return fmt.Errorf("invalid path pattern: %v", err)
	}

	for _, match := range matches {
		err := os.Remove(match)
		if err != nil {
			return err
		}
	}

	return nil
}

func RenameFilesByRegex(pathPattern string, regexPattern string, replacement string) error {
	// 编译正则表达式
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %v", err)
	}

	// 获取匹配的文件列表
	matches, err := filepath.Glob(pathPattern)
	if err != nil {
		return fmt.Errorf("invalid path pattern: %v", err)
	}

	// 遍历所有匹配的文件
	for _, filePath := range matches {
		// 检查是否为文件（而非目录）
		info, err := os.Stat(filePath)
		if err != nil {
			fmt.Printf("Warning: cannot stat %s: %v\n", filePath, err)
			continue
		}

		if info.IsDir() {
			continue
		}

		// 获取文件名
		filename := filepath.Base(filePath)

		// 使用正则表达式替换文件名
		newFilename := re.ReplaceAllString(filename, replacement)

		// 如果文件名没有变化，则跳过
		if newFilename == filename {
			continue
		}

		// 构造新的文件路径
		newFilePath := filepath.Join(filepath.Dir(filePath), newFilename)

		// 重命名文件
		err = os.Rename(filePath, newFilePath)
		if err != nil {
			fmt.Printf("Warning: cannot rename %s to %s: %v\n", filePath, newFilePath, err)
			continue
		}

		fmt.Printf("Renamed: %s -> %s\n", filePath, newFilePath)
	}

	return nil
}
