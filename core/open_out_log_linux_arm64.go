// +build linux,arm64

/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package core

import (
	"syscall"
)

// linux_arm64 doesn't have syscall.Dup2, so use
// the nearly identical syscall.Dup3 instead
func internalDup2(oldfd uintptr, newfd uintptr) error {
	return syscall.Dup3(int(oldfd), int(newfd), 0)
}
