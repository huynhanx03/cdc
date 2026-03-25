package utils

// ToProto converts a pointer to a struct to a pointer to another struct using a mapping function.
func ToProto[T any, P any](src *T, f func(*T) *P) *P {
	if src == nil {
		return nil
	}
	return f(src)
}
