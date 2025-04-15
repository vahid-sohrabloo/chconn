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

// DeleteFunc removes any elements from s for which del returns true,
// returning the modified slice.
// DeleteIndexFunc zeroes the elements between the new length and the original length.
func DeleteIndexFunc[S ~[]E, E any](s S, del func(i int) bool) S {
	i := 0
	for j := 0; j < len(s); j++ {
		if v := s[j]; !del(j) {
			s[i] = v
			i++
		}
	}
	clear(s[i:]) // zero/nil out the obsolete elements, for GC
	return s[:i]
}
