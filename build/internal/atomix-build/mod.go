// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix_build

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/atomix/atomix/build/internal/exec"
	"github.com/rogpeppe/go-internal/modfile"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func fetchMod(pathVersion string) (string, *modfile.File, error) {
	if pathVersion == "" || pathVersion == "." {
		log.Debug("Lookup local module go.mod")
		modFile, err := parseModFile("go.mod")
		if err != nil {
			return "", nil, err
		}
		log.Debug("Found local module go.mod")
		return ".", modFile, nil
	}

	path, version, err := splitPathVersion(pathVersion)
	if err != nil {
		return "", nil, err
	}

	if _, err := os.Stat("go.mod"); err == nil {
		log.Debug("Lookup local module go.mod")
		modFile, err := parseModFile("go.mod")
		if err != nil {
			return "", nil, err
		}
		if modFile.Module.Mod.Path == path {
			log.Debug("Found local module go.mod")
			return ".", modFile, nil
		}
	}

	path = filepath.Clean(path)
	for path != "" && path != "/" {
		localModPath := filepath.Join(path, "go.mod")
		log.Debugf("Lookup local module %s", path)
		if _, err := os.Stat(localModPath); err == nil {
			modFile, err := parseModFile(localModPath)
			if err != nil {
				return "", nil, err
			}
			log.Debugf("Found local module %s", path)
			return path, modFile, nil
		}
		log.Debugf("Local module not found")

		remoteModPath := joinPathVersion(path, version)
		log.Debugf("Lookup remote module %s", remoteModPath)
		modJSON, err := exec.Command("go", "mod", "download").
			Output("-json", remoteModPath)
		if err == nil {
			log.Debugf("Found remote module %s", remoteModPath)
			var modInfo goModInfo
			if err := json.Unmarshal(modJSON, &modInfo); err != nil {
				return "", nil, err
			}
			log.Debugf("Parsing module info %s", modInfo.GoMod)
			modFile, err := parseModFile(modInfo.GoMod)
			if err != nil {
				return "", nil, err
			}
			return modInfo.Dir, modFile, nil
		}
		log.Debugf("Remote module not found")
		path = filepath.Clean(filepath.Dir(path))
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
