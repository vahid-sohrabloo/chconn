package column

import "strings"

// JSONValue is a path-based value store for object-mode JSON inserts.
// Paths use dot notation (e.g. "a.b.c") to represent nested keys.
type JSONValue struct {
	valuesByPath map[string]any
}

// NewJSONValue creates an empty JSONValue.
func NewJSONValue() *JSONValue {
	return &JSONValue{
		valuesByPath: make(map[string]any),
	}
}

// SetValueAtPath sets a value at the given dot-separated path.
func (j *JSONValue) SetValueAtPath(path string, value any) {
	j.valuesByPath[path] = value
}

// ValueAtPath returns the value at the given path and whether it exists.
func (j *JSONValue) ValueAtPath(path string) (any, bool) {
	v, ok := j.valuesByPath[path]
	return v, ok
}

// ValuesByPath returns the underlying flat path map.
func (j *JSONValue) ValuesByPath() map[string]any {
	return j.valuesByPath
}

// NestedMap converts the flat dot-separated paths into a nested map[string]any.
func (j *JSONValue) NestedMap() map[string]any {
	result := make(map[string]any, len(j.valuesByPath))
	for path, val := range j.valuesByPath {
		setNestedValue(result, path, val)
	}
	return result
}

// setNestedValue sets a value in a nested map using a dot-separated path.
func setNestedValue(m map[string]any, path string, val any) {
	parts := strings.Split(path, ".")
	current := m
	for i := 0; i < len(parts)-1; i++ {
		key := parts[i]
		if next, ok := current[key]; ok {
			if nextMap, ok := next.(map[string]any); ok {
				current = nextMap
			} else {
				// overwrite non-map with a new map
				nextMap = make(map[string]any)
				current[key] = nextMap
				current = nextMap
			}
		} else {
			nextMap := make(map[string]any)
			current[key] = nextMap
			current = nextMap
		}
	}
	current[parts[len(parts)-1]] = val
}

// flattenMap flattens a nested map[string]any into dot-separated paths.
func flattenMap(m map[string]any, prefix string, out map[string]any) {
	for k, v := range m {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}
		if nested, ok := v.(map[string]any); ok {
			flattenMap(nested, path, out)
		} else {
			out[path] = v
		}
	}
}
