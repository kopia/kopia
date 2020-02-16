package policy

import "strings"

// DefaultPolicy is a default policy returned by policy tree in absence of other policies.
var DefaultPolicy = &Policy{
	FilesPolicy:         defaultFilesPolicy,
	RetentionPolicy:     defaultRetentionPolicy,
	CompressionPolicy:   defaultCompressionPolicy,
	ErrorHandlingPolicy: defaultErrorHandlingPolicy,
	SchedulingPolicy:    defaultSchedulingPolicy,
}

// Tree represents a node in the policy tree, where a policy can be
// defined. A nil tree is a valid tree with default policy.
type Tree struct {
	effective *Policy
	inherited bool
	children  map[string]*Tree
}

// DefinedPolicy returns policy that's been explicitly defined for tree node or nil if no policy was defined.
func (t *Tree) DefinedPolicy() *Policy {
	if t == nil || t.inherited {
		return nil
	}

	return t.effective
}

// EffectivePolicy returns policy that's been defined for this tree node or inherited from its parent.
func (t *Tree) EffectivePolicy() *Policy {
	if t == nil {
		return DefaultPolicy
	}

	return t.effective
}

// IsInherited returns true if the policy inherited to the given tree hode has been inherited from its parent.
func (t *Tree) IsInherited() bool {
	if t == nil {
		return true
	}

	return t.inherited
}

// Child gets a subtree for an entry with a given name.
func (t *Tree) Child(name string) *Tree {
	if t == nil {
		return nil
	}

	parts := strings.Split(name, "/")
	switch len(parts) {
	case 1:
		if name == "." || name == "" {
			return t
		}

		ch := t.children[name]
		if ch != nil {
			return ch
		}

		// tree with no children, we can just reuse current node
		if len(t.children) == 0 && t.inherited {
			return t
		}

		return &Tree{effective: t.effective, inherited: true}

	default:
		ch := t
		for _, p := range parts {
			ch = ch.Child(p)
		}

		return ch
	}
}

// BuildTree builds a policy tree from the given map of paths to policies.
// Each path must be relative and start with "." and be separated by slashes.
func BuildTree(defined map[string]*Policy, defaultPolicy *Policy) *Tree {
	return buildTreeNode(defined, ".", defaultPolicy)
}

func buildTreeNode(defined map[string]*Policy, path string, defaultPolicy *Policy) *Tree {
	n := &Tree{
		effective: defined[path],
	}
	if n.effective == nil {
		n.effective = defaultPolicy
		n.inherited = true
	}

	children := childrenWithPrefix(defined, path+"/")
	if len(children) > 0 {
		n.children = map[string]*Tree{}

		for childName, descendants := range children {
			n.children[childName] = buildTreeNode(descendants, path+"/"+childName, n.effective)
		}
	}

	return n
}

func childrenWithPrefix(m map[string]*Policy, path string) map[string]map[string]*Policy {
	result := map[string]map[string]*Policy{}

	for k, v := range m {
		if !strings.HasPrefix(k, path) {
			continue
		}

		childName := strings.Split(k[len(path):], "/")[0]
		if result[childName] == nil {
			result[childName] = map[string]*Policy{}
		}

		result[childName][k] = v
	}

	return result
}
