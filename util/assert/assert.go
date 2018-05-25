// Package assert contains functions for making assertions in unit tests
// From github.com/docker/docker/pkg/testutil/assert
package assert

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"bytes"
	"time"
)

// TestingT is an interface which defines the methods of testing.T that are
// required by this package
type TestingT interface {
	Fatalf(string, ...interface{})
}

// Equal compare the actual value to the expected value and fails the test if
// they are not equal.
func Equal(t TestingT, actual, expected interface{}, msg string) {
	if expected != actual {
		fatal(t, "Expected '%v' (%T) got '%v' (%T). msg:%s", expected, expected, actual, actual, msg)
	}
}

// NotEqual compare the actual value to the expected value and fails the test if
// they are equal.
func NotEqual(t TestingT, actual, expected interface{}, msg string) {
	if expected == actual {
		fatal(t, "Not Expected '%v' (%T) got '%v' (%T). msg:%v", expected, expected, actual, actual, msg)
	}
}

//EqualStringSlice compares two slices and fails the test if they do not contain
// the same items.
func EqualStringSlice(t TestingT, actual, expected []string) {
	if len(actual) != len(expected) {
		fatal(t, "Expected (length %d): %q\nActual (length %d): %q",
			len(expected), expected, len(actual), actual)
	}
	for i, item := range actual {
		if item != expected[i] {
			fatal(t, "Slices differ at element %d, expected %q got %q",
				i, expected[i], item)
		}
	}
}

// NilError asserts that the error is nil, otherwise it fails the test.
func NilError(t TestingT, err error) {
	if err != nil {
		fatal(t, "Expected no error, got: %s", err.Error())
	}
}

// DeepEqual compare the actual value to the expected value and fails the test if
// they are not "deeply equal".
func DeepEqual(t TestingT, actual, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		fatal(t, "Expected '%v' (%T) got '%v' (%T)", expected, expected, actual, actual)
	}
}

// Error asserts that error is not nil, and contains the expected text,
// otherwise it fails the test.
func Error(t TestingT, err error, contains string) {
	if err == nil {
		fatal(t, "Expected an error, but error was nil")
	}

	if !strings.Contains(err.Error(), contains) {
		fatal(t, "Expected error to contain '%s', got '%s'", contains, err.Error())
	}
}

// Contains asserts that the string contains a substring, otherwise it fails the
// test.
func Contains(t TestingT, actual, contains string) {
	if !strings.Contains(actual, contains) {
		fatal(t, "Expected '%s' to contain '%s'", actual, contains)
	}
}

// NotNil fails the test if the object is nil
func NotNil(t TestingT, obj interface{}) {
	if obj == nil || reflect.ValueOf(obj).IsNil() {
		fatal(t, "Expected non-nil value. %t", obj)
	}
}

// Nil fails the test if the object is not nil
func Nil(t TestingT, obj interface{}) {
	if obj != nil {
		if !reflect.ValueOf(obj).IsNil() {
			fatal(t, "Expected nil value.")
		}
	}
}

// True fails the test if the object is false
func True(t TestingT, obj bool) {
	if obj == false {
		fatal(t, "Expected true value.")
	}
}

// False fails the test if the object is true
func False(t TestingT, obj bool) {
	if obj == true {
		fatal(t, "Expected false value.")
	}
}

func Less(t TestingT, x, y interface{}) {
	v1Type := reflect.TypeOf(x)
	v2Type := reflect.TypeOf(y)

	if v1Type.Kind() != v2Type.Kind() {
		fatal(t, "Needs two same type, but %s != %s", v1Type.Kind(), v2Type.Kind())
	}
	l, err := less(x, y)
	if err != nil {
		fatal(t, "less failed, err %v", err)
	}
	if !l {
		fatal(t, "Expected less than expected value.")
	}
}

func fatal(t TestingT, format string, args ...interface{}) {
	t.Fatalf(errorSource()+format, args...)
}

// See testing.decorate()
func errorSource() string {
	_, filename, line, ok := runtime.Caller(3)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s:%d: ", filepath.Base(filename), line)
}

type T struct {}

func (t *T)Fatalf(format string, a ...interface{}) {
	panic(fmt.Sprintf(format, a...))
}

var testingT *T = &T{}

func Must(b bool) {
	if b {
		return
	}
	fatal(testingT, "assertion failed")
}

func MustNoError(err error) {
	if err == nil {
		return
	}
	fatal(testingT, "error happens, assertion failed %v", err)
}


// v1 and v2 must have the same type
// return >0 if v1 > v2
// return 0 if v1 = v2
// return <0 if v1 < v2
// now we only support int, uint, float64, string and []byte comparison
func compare(v1 interface{}, v2 interface{}) (int, error) {
	value1 := reflect.ValueOf(v1)
	value2 := reflect.ValueOf(v2)

	switch v1.(type) {
	case int, int8, int16, int32, int64:
		a1 := value1.Int()
		a2 := value2.Int()
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	case uint, uint8, uint16, uint32, uint64:
		a1 := value1.Uint()
		a2 := value2.Uint()
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	case float32, float64:
		a1 := value1.Float()
		a2 := value2.Float()
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	case string:
		a1 := value1.String()
		a2 := value2.String()
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	case []byte:
		a1 := value1.Bytes()
		a2 := value2.Bytes()
		return bytes.Compare(a1, a2), nil
	case time.Time:
		a1 := v1.(time.Time)
		a2 := v2.(time.Time)
		if a1.After(a2) {
			return 1, nil
		} else if a1.Equal(a2) {
			return 0, nil
		}
		return -1, nil
	case time.Duration:
		a1 := v1.(time.Duration)
		a2 := v2.(time.Duration)
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	default:
		return 0, fmt.Errorf("type %T is not supported now", v1)
	}
}

func less(v1 interface{}, v2 interface{}) (bool, error) {
	n, err := compare(v1, v2)
	if err != nil {
		return false, err
	}

	return n < 0, nil
}

func lessEqual(v1 interface{}, v2 interface{}) (bool, error) {
	n, err := compare(v1, v2)
	if err != nil {
		return false, err
	}

	return n <= 0, nil
}

func greater(v1 interface{}, v2 interface{}) (bool, error) {
	n, err := compare(v1, v2)
	if err != nil {
		return false, err
	}

	return n > 0, nil
}

func greaterEqual(v1 interface{}, v2 interface{}) (bool, error) {
	n, err := compare(v1, v2)
	if err != nil {
		return false, err
	}

	return n >= 0, nil
}
