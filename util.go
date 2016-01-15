package main

import (
	"fmt"
)

func set(s []string, index int, value string) []string {
	for len(s) <= index {
		s = append(s, "")
	}
	s[index] = value
	return s
}

func hashToStr(h []byte) string {
	var s string
	s = ""
	for _, b := range h {
		s += fmt.Sprintf("%02x", b)
	}

	return s
}

func parseHashStr(s string) ([]byte, error) {
	var res = make([]byte, 0)
	for n := 0; n < len(s); n += 2 {
		var p string
		if n+2 <= len(s) {
			p = s[n : n+2]
		} else {
			p = s[n : n+1]
		}
		var v byte
		_, err := fmt.Sscanf(p, "%02x", &v)
		if err != nil {
			return nil, err
		}
		res = append(res, v)
	}
	return res, nil
}
