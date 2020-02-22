package main

import "encoder/gob"

//Create result buffer
var buf bytes.Buffer
//Create encoder
encoder := gob.NewEncoder(&buf)
var obj SomeType
result := buf.Bytes()
