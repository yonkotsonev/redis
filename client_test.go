package redis

import (
	"reflect"
	"testing"
)

func TestSetGetDel(t *testing.T) {
	client := NewClient("localhost", 6379)

	resp, err := client.Set("test", "test")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	if resp != "OK" {
		t.Error("Wrong response to set command")
	}

	resp, err = client.Get("test")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	if resp != "test" {
		t.Error("Wrong response to get command")
	}

	resp, err = client.Del("test")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	if resp != 1 {
		t.Error("Wrong response to del command")
	}
}

func TestSets(t *testing.T) {
	client := NewClient("localhost", 6379)

	resp, err := client.Sadd("test", "test", "1", "2")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	if resp != 3 {
		t.Error("Wrong response to set command")
	}

	resp, err = client.Sadd("test", "3", "4", "5")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	if resp != 3 {
		t.Error("Wrong response to set command")
	}

	resp, err = client.SisMember("test", "3")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	if resp != 1 {
		t.Error("Wrong response to SisMember command")
	}

	resp, err = client.SisMember("test", "13")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	if resp != 0 {
		t.Error("Wrong response to SisMember command")
	}

	resp, err = client.Srem("test", "test")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	if resp != 1 {
		t.Error("Wrong response to Srem command")
	}

	resp, err = client.Smembers("test")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	expected := []string{"1", "2", "3", "4", "5"}
	if compare(resp, expected) == false {
		t.Errorf("Wrong(%v), Expected(%v)", resp, expected)
	}

	resp, err = client.Del("test")
	if err != nil {
		t.Errorf("Error sending request: %v", err)
	}

	if resp != 1 {
		t.Error("Wrong response to del command")
	}
}

func TestPipeline(t *testing.T) {
	client := NewClient("localhost", 6379)
	client.Pipeline()

	resp, err := client.Set("test", "1")
	if resp != nil || err != nil {
		t.Errorf("Error sending pipelined request: Error(%v), Response(%v)", err, resp)
	}

	resp, err = client.Get("test")
	if resp != nil || err != nil {
		t.Errorf("Error sending pipelined request: Error(%v), Response(%v)", err, resp)
	}

	resp, err = client.Del("test")
	if resp != nil || err != nil {
		t.Errorf("Error sending pipelined request: Error(%v), Response(%v)", err, resp)
	}

	resp, err = client.Execute()
	if err != nil {
		t.Errorf("Error During Pipeline execution (%v)", err)
	}

	expected := []interface{}{"OK", "1", 1}
	if compare(resp, expected) == false {
		t.Errorf("Wrong(%t), Expected(%t)", resp, expected)
	}
}

func TestListBlpopRpush(t *testing.T) {
	client := NewClient("localhost", 6379)

	resp, err := client.Blpop("test", 1)
	if resp != nil || err != nil {
		t.Errorf("Error sending blpop request for empty queue: Error(%v), Response(%v)", err, resp)
	}

	resp, err = client.Rpush("test", "test")
	if resp != 1 || err != nil {
		t.Errorf("Error sending rpush request: Error(%v), Response(%v)", err, resp)
	}

	resp, err = client.Blpop("test", 1)
	expected := []string{"test", "test"}
	if err != nil || !compare(resp, expected) {
		t.Errorf("Error sending blpop request for queue with \"test\": Error(%v), Response(%v)", err, resp)
	}
}

func compare(a interface{}, b interface{}) bool {
	vA := reflect.ValueOf(a)
	vB := reflect.ValueOf(b)

	if vA.Kind() != vB.Kind() {
		return false
	}

	if vA.Len() != vB.Len() {
		return false
	}

	switch vA.Kind() {
	case reflect.Slice:
		helperMap := make(map[interface{}]int)

		for i := 0; i < vA.Len(); i++ {
			k := vA.Index(i).Interface()
			if val, ok := helperMap[k]; ok {
				helperMap[k] = val + 1
			} else {
				helperMap[k] = 1
			}
		}

		for i := 0; i < vB.Len(); i++ {
			k := vB.Index(i).Interface()
			if val, ok := helperMap[k]; ok {
				if val-1 < 0 {
					return false
				} else {
					helperMap[k] = val - 1
				}
			} else {
				return false
			}
		}

		return true
	}

	return true
}
