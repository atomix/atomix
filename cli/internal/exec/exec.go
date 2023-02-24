// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"io"
	"os"
	"os/exec"
	"strings"
)

func Command(name string, args ...string) *Cmd {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return &Cmd{
		cmd: cmd,
	}
}

type Cmd struct {
	cmd *exec.Cmd
}

func (c *Cmd) Stdout(stdout io.Writer) *Cmd {
	c.cmd.Stdout = stdout
	return c
}

func (c *Cmd) Stderr(stderr io.Writer) *Cmd {
	c.cmd.Stderr = stderr
	return c
}

func (c *Cmd) Dir(dir string) *Cmd {
	c.cmd.Dir = dir
	return c
}

func (c *Cmd) Env(env ...string) *Cmd {
	c.cmd.Env = append(c.cmd.Env, env...)
	return c
}

func (c *Cmd) Run(args ...string) error {
	c.cmd.Args = append(c.cmd.Args, args...)
	println(strings.Join(c.cmd.Args, " "))
	return c.cmd.Run()
}

func (c *Cmd) Output(args ...string) ([]byte, error) {
	c.cmd.Stdout = nil
	c.cmd.Args = append(c.cmd.Args, args...)
	println(strings.Join(c.cmd.Args, " "))
	return c.cmd.Output()
}
