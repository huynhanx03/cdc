package utils

// DerefString returns the value of the string pointer or a default value if nil.
func DerefString(ptr *string, defaultValue string) string {
	if ptr == nil {
		return defaultValue
	}
	return *ptr
}
