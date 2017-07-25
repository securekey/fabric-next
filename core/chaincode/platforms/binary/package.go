/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binary

import (
	"fmt"
	"strings"

	"os"
	"path/filepath"

	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("golang-platform")

type CodeDescriptor struct {
	Gopath, Pkg string
	Cleanup     func()
}

type SourceDescriptor struct {
	Name, Path string
	Info       os.FileInfo
}
type SourceMap map[string]SourceDescriptor

type Sources []SourceDescriptor

func (s Sources) Len() int {
	return len(s)
}

func (s Sources) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Sources) Less(i, j int) bool {
	return strings.Compare(s[i].Name, s[j].Name) < 0
}

func findSource(gopath, pkg string) (SourceMap, error) {
	sources := make(SourceMap)
	tld := filepath.Join(gopath, "src", pkg)
	walkFn := func(path string, info os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if info.IsDir() {
			if path == tld {
				// We dont want to import any directories, but we don't want to stop processing
				// at the TLD either.
				return nil
			}

			// Do not recurse
			logger.Debugf("skipping dir: %s", path)
			return filepath.SkipDir
		}

		ext := filepath.Ext(path)

		if len(ext) > 0 {
			// we only want 'fileTypes' source files at this point
			return fmt.Errorf("Unexpected file types found in path : %s", path)
		}

		name, err := filepath.Rel(gopath, path)
		if err != nil {
			return fmt.Errorf("error obtaining relative path for %s: %s", path, err)
		}

		sources[name] = SourceDescriptor{Name: name, Path: path, Info: info}

		return nil
	}

	if err := filepath.Walk(tld, walkFn); err != nil {
		return nil, fmt.Errorf("Error walking directory: %s", err)
	}

	return sources, nil
}
