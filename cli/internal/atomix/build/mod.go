// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/atomix/atomix/cli/internal/exec"
	"github.com/rogpeppe/go-internal/modfile"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// getLocalModFromMain determines whether the module for the given main resides on the local file system and
// returns the absolute path to the local module directory for the given main and the relative path from the
// module directory root.
// Examples:
//
//	getLocalModFromMain("./cmd/atomix") from module root at /Users/dev/atomix => ("/Users/dev/atomix", *modfile.File, true, nil)
//	getLocalModFromMain("github.com/atomix/atomix/cli/cmd/atomix") from github.com/atomix/atomix/cli module root at /Users/dev/atomix => ("/Users/dev/atomix", *modfile.File, true, nil)
//	getLocalModFromMain("github.com/atomix/atomix/cli/cmd/atomix@v1.0.0") from github.com/atomix/atomix/cli module root => ("", nil, false, nil)
//	getLocalModFromMain("github.com/atomix/atomix/cli/cmd/atomix") from outside github.com/atomix/atomix/cli module root => ("", nil, false, nil)
//	getLocalModFromMain("github.com/atomix/atomix/cli/cmd/atomix@v1.0.0") from outside github.com/atomix/atomix/cli module root => ("", nil, false, nil)
func getLocalModFromMain(path string) (string, *modfile.File, bool, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", nil, false, err
	}
	return getDirModFromMain(dir, path)
}

func getDirModFromMain(dir, path string) (string, *modfile.File, bool, error) {
	path = filepath.Clean(path)
	if modFile, ok, err := getModFile(dir); err != nil {
		return "", nil, false, err
	} else if ok {
		var mainPath string
		if path != modFile.Module.Mod.Path && strings.HasPrefix(path, modFile.Module.Mod.Path) {
			mainPath = filepath.Join(dir, path[len(modFile.Module.Mod.Path)+1:])
		} else {
			mainPath = filepath.Join(dir, path)
		}
		if _, err := os.Stat(mainPath); err != nil {
			return "", nil, false, nil
		}
		return dir, modFile, true, nil
	}
	return "", nil, false, nil
}

// getLocalMod determines whether the given path resolves to a Go module on the local file system. If so,
// it returns the absolute path to the module on the local file system.
// Examples:
//
//	getLocalMod(".") from root of a valid module at /Users/dev/atomix => ("/Users/dev/atomix", *modfile.File, true, nil)
//	getLocalMod("../../some") where ../../some resolves to the root of a valid module at /Users/dev/some => ("/Users/dev/some", *modfile.File, true, nil)
//	getLocalMod("github.com/atomix/atomix/cli") from github.com/atomix/atomix/cli module root => ("/Users/dev/atomix", *modfile.File, true, nil)
//	getLocalMod("github.com/atomix/atomix/cli@v1.0.0") from github.com/atomix/atomix/cli module root => ("", nil, false, nil)
//	getLocalMod("github.com/atomix/atomix/cli") from outside github.com/atomix/atomix/cli module root => ("", nil, false, nil)
//	getLocalMod("github.com/atomix/atomix/cli@v1.0.0") from outside github.com/atomix/atomix/cli module root => ("", nil, false, nil)
func getLocalMod(path string) (string, *modfile.File, bool, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", nil, false, err
	}
	return getDirMod(dir, path)
}

func getDirMod(dir, path string) (string, *modfile.File, bool, error) {
	path = filepath.Clean(path)
	if modFile, ok, err := getModFile(dir); err != nil {
		return "", nil, false, err
	} else if ok && modFile.Module.Mod.Path == path {
		return dir, modFile, true, nil
	}
	modDir, err := filepath.Abs(filepath.Join(dir, path))
	if err != nil {
		return "", nil, false, err
	}
	if modFile, ok, err := getModFile(modDir); err != nil {
		return "", nil, false, err
	} else if ok {
		return modDir, modFile, true, nil
	}
	return "", nil, false, nil
}

func getModFile(dir string) (*modfile.File, bool, error) {
	path := filepath.Join(dir, "go.mod")
	if info, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, false, err
		}
		return nil, false, nil
	} else if info.IsDir() {
		return nil, false, nil
	}
	if modFile, err := parseModFile(path); err != nil {
		return nil, false, err
	} else {
		return modFile, true, nil
	}
}

func downloadMod(pathVersion string) (string, *modfile.File, bool, error) {
	modJSON, err := exec.Command("go", "mod", "download", "-json").Output(pathVersion)
	if err != nil {
		return "", nil, false, nil
	}
	var modInfo goModInfo
	if err := json.Unmarshal(modJSON, &modInfo); err != nil {
		return "", nil, false, err
	}
	modFile, err := parseModFile(modInfo.GoMod)
	if err != nil {
		return "", nil, false, err
	}
	return modInfo.Dir, modFile, true, nil
}

func fetchModFromMain(pathVersion string) (string, *modfile.File, error) {
	if modDir, modFile, ok, err := getLocalModFromMain(pathVersion); err != nil {
		return "", nil, err
	} else if ok {
		return modDir, modFile, nil
	}

	mainPath, modVersion, err := splitPathVersion(pathVersion)
	if err != nil {
		return "", nil, err
	}

	modPath := mainPath
	for modPath != "." {
		modPathVersion := joinPathVersion(modPath, modVersion)
		if modDir, modFile, ok, err := downloadMod(modPathVersion); err != nil {
			return "", nil, err
		} else if ok {
			mainDir, err := filepath.Rel(modFile.Module.Mod.Path, mainPath)
			if err != nil {
				return "", nil, err
			}
			if _, err := os.Stat(filepath.Join(modDir, mainDir)); err != nil {
				if !os.IsNotExist(err) {
					return "", nil, err
				}
			} else {
				return modDir, modFile, nil
			}
		}
		modPath = filepath.Dir(modPath)
	}
	return "", nil, errors.New(fmt.Sprintf("could not resolve module %s", pathVersion))
}

func fetchMod(pathVersion string) (string, *modfile.File, error) {
	if modDir, modFile, ok, err := getLocalMod(pathVersion); err != nil {
		return "", nil, err
	} else if ok {
		return modDir, modFile, nil
	}
	if modDir, modFile, ok, err := downloadMod(pathVersion); err != nil {
		return "", nil, err
	} else if ok {
		return modDir, modFile, err
	}
	return "", nil, errors.New(fmt.Sprintf("could not resolve module %s", pathVersion))
}

func copyDir(sourceDir, destDir string) error {
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		sourcePath := filepath.Join(sourceDir, entry.Name())
		destPath := filepath.Join(destDir, entry.Name())

		fileInfo, err := os.Stat(sourcePath)
		if err != nil {
			return err
		}

		if fileInfo.IsDir() {
			if err := os.MkdirAll(destPath, os.ModePerm); err != nil {
				return err
			}
			if err := copyDir(sourcePath, destPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(sourcePath, destPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyFile(sourcePath, destPath string) error {
	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return err
	}
	return nil
}

func parseModFile(modPath string) (*modfile.File, error) {
	modBytes, err := os.ReadFile(modPath)
	if err != nil {
		return nil, err
	}
	modFile, err := modfile.Parse(modPath, modBytes, nil)
	if err != nil {
		return nil, err
	}
	return modFile, nil
}

func splitPathVersion(pathVersion string) (string, string, error) {
	pathParts := strings.Split(pathVersion, "@")
	switch len(pathParts) {
	case 1:
		return pathParts[0], "", nil
	case 2:
		return pathParts[0], pathParts[1], nil
	default:
		return "", "", errors.New("malformed path@version pair")
	}
}

func joinPathVersion(path string, version string) string {
	if version == "" {
		return path
	}
	return fmt.Sprintf("%s@%s", path, version)
}

type goModInfo struct {
	Path     string
	Version  string
	Error    string
	Info     string
	GoMod    string
	Zip      string
	Dir      string
	Sum      string
	GoModSum string
}
