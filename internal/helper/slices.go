package helper

func ResetSlice[S ~[]E, E any](s S, n int, needClear bool) S {
	if len(s) >= n {
		if needClear {
			clear(s[:n])
		}
		return s[:n]
	}
	return make(S, n)
}
