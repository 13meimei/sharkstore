package util

import (
	"testing"
	"fmt"
)

func Test_Uuid(t *testing.T)  {
	uuid := NewUuid()
	fmt.Println(uuid)
}
