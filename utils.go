package main

import "log"

// assert checks a condition and panics with a message if the condition is false
func assert(condition bool, message ...string) {
	if !condition {
		if len(message) > 0 {
			log.Panicf("Assertion failed: %s", message[0])
		} else {
			log.Panic("Assertion failed")
		}
	}
}
