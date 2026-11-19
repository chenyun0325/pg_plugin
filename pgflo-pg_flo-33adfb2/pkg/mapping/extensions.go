package mapping

import (
	"fmt"
	"sync"
)

var extensionRegistry = struct {
	sync.RWMutex
	functions map[string]Function
}{functions: make(map[string]Function)}

// RegisterExtension installs a compiled, deterministic Go function for every
// subsequently created engine, including job preview and the replay sink. Call
// this during binary initialization, before loading jobs or starting workers.
// Change mapping.version whenever a function's semantics change.
func RegisterExtension(name string, function Function) error {
	if name == "" || function == nil {
		return fmt.Errorf("function name and implementation are required")
	}
	builtins := &Engine{functions: make(map[string]Function)}
	builtins.registerBuiltins()
	if _, exists := builtins.functions[name]; exists {
		return fmt.Errorf("cannot replace builtin %s", name)
	}
	extensionRegistry.Lock()
	defer extensionRegistry.Unlock()
	if _, exists := extensionRegistry.functions[name]; exists {
		return fmt.Errorf("extension %s already registered", name)
	}
	extensionRegistry.functions[name] = function
	return nil
}

func (e *Engine) registerExtensions() {
	extensionRegistry.RLock()
	defer extensionRegistry.RUnlock()
	for name, function := range extensionRegistry.functions {
		e.functions[name] = function
	}
}
