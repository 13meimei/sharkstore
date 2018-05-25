package common

import (
	"strconv"
	"fmt"
)

func Argument(d map[string]interface{}, name string) (string, bool) {
	if d[name] != nil {
		if s, ok := d[name].(string); ok {
			if s != "" {
				return s, true
			}
			panic(fmt.Sprintf("option %s requires an argument\n", name))
		} else {
			panic(fmt.Sprintf("option %s isn't a valid string\n", name))
		}
	}
	return "", false
}

func ArgumentMust(d map[string]interface{}, name string) string {
	s, ok := Argument(d, name)
	if ok {
		return s
	}
	panic(fmt.Sprintf("option %s is required\n", name))
	return ""
}

func ArgumentInteger(d map[string]interface{}, name string) (int, bool) {
	if s, ok := Argument(d, name); ok {
		n, err := strconv.Atoi(s)
		if err != nil {
			panic(fmt.Sprintf("option %s isn't a valid integer. err:[%s]\n", name, err.Error()))
		}
		return n, true
	}
	return 0, false
}

func ArgumentIntegerMust(d map[string]interface{}, name string) int {
	n, ok := ArgumentInteger(d, name)
	if ok {
		return n
	}
	panic(fmt.Sprintf("option %s is required\n", name))
	return 0
}
