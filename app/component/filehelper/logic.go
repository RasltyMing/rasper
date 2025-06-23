package filehelper

import (
	"archive/zip"
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"raselper/app/base/fileu"
	"strings"
)

func RunLogicByConfig(config *ConfigFileHelper) (*LogicStruct, error) {
	switch config.command {
	case "config":
		fmt.Printf("%+v\n", config)
		return nil, nil
	case "zip":
		return Zip(config)
	case "copy":
		return CopyFiles(config)
	case "rname":
		return ReplaceName(config)
	case "rfile":
		return ReplaceFileData(config)
	case "filter":
		return FilterFile(config)
	case "help":
		fmt.Println("config")
		fmt.Println("zip")
		fmt.Println("copy")
		fmt.Println("replace_name")
		fmt.Println("replace_file")
		fmt.Println("filter")
		return nil, nil
	}

	return nil, errors.New("command:" + config.command + " not found")
}

func Zip(helper *ConfigFileHelper) (*LogicStruct, error) {
	archive, err := os.Create(helper.targetPath)
	if err != nil {
		return nil, err
	}
	defer archive.Close()

	zipWriter := zip.NewWriter(archive)
	defer zipWriter.Close()

	isEmpty := true // zip文件是否为空?

	err = filepath.Walk(helper.sourcePath, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() || path == helper.targetPath { // 目录跳过, 目标路径跳过
			return nil
		}

		// zip文件
		fmt.Println("zip fileu ", path, " to ", helper.targetPath)
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
		if err := os.Remove(helper.targetPath); err != nil {
			fmt.Println("err: ", err)
		}
	}

	return nil, nil
}

func ReplaceName(helper *ConfigFileHelper) (*LogicStruct, error) {
	if len(helper.source) < 6 {
		return nil, errors.New("source path not found")
	}

	sourcePath, err := filepath.Abs(helper.source[3])
	if err != nil {
		return nil, err
	}
	replaceStr := helper.source[4]
	replacedStr := helper.source[5]

	err = filepath.Walk(sourcePath, func(path string, info fs.FileInfo, err error) error {
		if !info.IsDir() {
			// Get the base name of the file
			baseName := filepath.Base(path)
			// Replace the old name with new name
			newBaseName := strings.Replace(baseName, replaceStr, replacedStr, -1)

			if newBaseName != baseName {
				// Construct the new path
				dir := filepath.Dir(path)
				newPath := filepath.Join(dir, newBaseName)

				// Rename the file
				err := os.Rename(path, newPath)
				if err != nil {
					fmt.Printf("Error renaming %s to %s: %v\n", path, newPath, err)
					return err
				}
				fmt.Printf("Renamed %s to %s\n", path, newPath)
			}
		}
		return nil
	})

	return nil, nil
}

func ReplaceFileData(helper *ConfigFileHelper) (*LogicStruct, error) {
	if len(helper.source) < 6 {
		return nil, errors.New("source path not found")
	}

	sourcePath, err := filepath.Abs(helper.source[3])
	if err != nil {
		return nil, err
	}
	replaceStr := helper.source[4]
	replacedStr := helper.source[5]

	err = filepath.Walk(sourcePath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			// Read the file content
			content, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("error reading file %s: %v", path, err)
			}

			// Replace the content
			newContent := strings.ReplaceAll(string(content), replaceStr, replacedStr)

			// Write the new content back to the file
			err = os.WriteFile(path, []byte(newContent), info.Mode())
			if err != nil {
				return fmt.Errorf("error writing file %s: %v", path, err)
			}

			fmt.Printf("Replaced content in file: %s\n", path)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error replacing file data: %v", err)
	}

	return nil, nil
}

// CopyFiles 复制一个目录下的所有文件
func CopyFiles(helper *ConfigFileHelper) (*LogicStruct, error) {
	sourcePath, _ := filepath.Abs(helper.sourcePath)

	err := filepath.Walk(sourcePath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			println(err.Error())
		}

		file, _ := os.Stat(path)
		if file.IsDir() {
			return nil
		}
		_targetPath := filepath.Join(helper.targetPath, strings.Replace(path, sourcePath, "", 1))
		err = fileu.CopyFile(sourcePath, _targetPath, nil)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return nil, nil
}

// CopyFile 复制单个文件
func CopyFile(sourcePath string, targetPath string) error {
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

// FilterFile reads a source file, filters lines containing a keyword, and writes them to an output file.
func FilterFile(helper *ConfigFileHelper) (*LogicStruct, error) {
	sourceFile, err := os.Open(helper.filterSourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file %s: %w", helper.filterSourcePath, err)
	}
	defer sourceFile.Close()

	outputFile, err := os.Create(helper.filterOutputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file %s: %w", helper.filterOutputPath, err)
	}
	defer outputFile.Close()

	reader := bufio.NewReader(sourceFile)
	writer := bufio.NewWriter(outputFile)
	linesWritten := 0
	defer writer.Flush()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading source file: %w", err)
		}

		if strings.Contains(line, helper.filterKeyword) {
			_, err := writer.WriteString(line)
			if err != nil {
				return nil, fmt.Errorf("failed to write to output file: %w", err)
			}
			linesWritten++
		}
	}

	fmt.Printf("Filtered %d lines from %s containing '%s' to %s\n", linesWritten, helper.filterSourcePath, helper.filterKeyword, helper.filterOutputPath)
	return nil, nil
}
