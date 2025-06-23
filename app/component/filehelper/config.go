package filehelper

import (
	"errors"
	"strings"
	"time"
)

type ConfigFileHelper struct {
	source     []string
	sourcePath string // -s
	targetPath string // -t
	command    string
	replace    []string // -r 替换

	// New fields for the "filter" command
	filterSourcePath string
	filterKeyword    string
	filterOutputPath string
}

func ReadConfig(fullArgs []string) (*ConfigFileHelper, error) {
	config := &ConfigFileHelper{source: fullArgs}

	// fullArgs is the complete os.Args or equivalent: ["raselper", "filehelper", "command", "arg1", ...]
	if len(fullArgs) < 3 { // Need at least program name, "filehelper", and a command
		return nil, errors.New("missing filehelper command")
	}

	// Slice the arguments to start from the actual filehelper subcommand (e.g., "filter", "copy")
	// Now configList is: ["command", "arg1", "arg2", ...]
	configList := fullArgs[2:]

	if len(configList) == 0 {
		return nil, errors.New("missing filehelper subcommand")
	}

	config.command = configList[0] // Set the command based on the first element of the sliced list

	switch config.command {
	case "filter":
		if len(configList) < 4 { // filter source_file keyword output_file
			return nil, errors.New("usage: filehelper filter <source_file> <keyword> <output_file>")
		}
		config.filterSourcePath = configList[1]
		config.filterKeyword = configList[2]
		config.filterOutputPath = configList[3]
		// For "filter", we have explicitly parsed the arguments, so we can return early.
		return config, nil
	default:
		// For other commands, process the arguments starting from index 1 (after the command itself)
		// using the existing flag and positional argument parsing logic.
		// The original loop also assigned 'command' if it was empty, but it's already set here.
		// So the 'if config.command == ""' condition will be skipped.
		// This will correctly assign targetPath and sourcePath for non-flagged arguments.

		for i := 1; i < len(configList); i++ { // Start from index 1 to skip the command itself
			subStr := configList[i]
			if strings.HasPrefix(subStr, "-") { // Is a flag
				switch subStr {
				case "-s":
					if len(configList) <= i+1 { // Parameter not exist?
						return nil, errors.New("param -s not exist")
					}
					config.sourcePath = configList[i+1]
					i++ // Skip the value part
				case "-t":
					if len(configList) <= i+1 { // Parameter not exist?
						return nil, errors.New("param -t not exist")
					}
					config.targetPath = configList[i+1]
					i++ // Skip the value part
				case "-r":
					if len(configList) <= i+1 { // Parameter not exist?
						return nil, errors.New("param -r not exist")
					}
					config.replace = append(config.replace, configList[i+1])
					i++ // Skip the value part
				default:
					return nil, errors.New("unknown parameter: " + subStr)
				}
			} else { // Not a flag, is a positional argument
				// This part of the logic needs to be careful because the original code
				// assigned 'targetPath' then 'sourcePath' in sequence for non-flagged arguments.
				// However, for commands like `copy source.txt target.txt`, `sourcePath` and `targetPath` might be used.
				// The original code was `if config.targetPath == "" { ... } if config.sourcePath == "" { ... }`.
				// This means the first free parameter goes to `targetPath`, second to `sourcePath`.
				// This is opposite of what `copy source target` would normally imply.
				// Let's preserve this existing behavior for old commands.
				if config.targetPath == "" {
					config.targetPath = subStr
				} else if config.sourcePath == "" {
					config.sourcePath = subStr
				}
				// If there are more unflagged parameters, they are ignored by the current original logic.
			}
		}

		// Apply time format to targetPath for existing commands, as in original.
		// This block was outside the loop in original `ReadConfig`.
		originalPath := config.targetPath
		originalPath = strings.ReplaceAll(originalPath, "YYYY", "2006")
		originalPath = strings.ReplaceAll(originalPath, "MM", "01")
		originalPath = strings.ReplaceAll(originalPath, "DD", "02")
		originalPath = strings.ReplaceAll(originalPath, "hh", "15")
		originalPath = strings.ReplaceAll(originalPath, "mm", "04")
		originalPath = strings.ReplaceAll(originalPath, "ss", "05")
		format := time.Now().Format(originalPath)
		config.targetPath = format

		return config, nil
	}
}
