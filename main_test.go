package main

import "testing"

func Test_splitFilePath(t *testing.T) {
	tests := []string{"QCLCD201712", "testingthisfile201712", "test201712", "201712"}
	result := "201712"

	for i := 0; i < len(tests); i++ {
		v := splitFilePath(tests[i])
		if v != result {
			t.Errorf("result was %v, not %v", tests[i], result)
		}
	}
}
