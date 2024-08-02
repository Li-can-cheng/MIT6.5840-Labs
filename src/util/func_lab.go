package main

import (
	"6.5840/mr"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

func splitContentsIntoAnArrayOfWords() {
	contents := "hello world 123 haha!"
	fmt.Println(contents)
	// 一个字母，是字母就false。
	// split contents into an array of words.
	words := strings.FieldsFunc(contents, func(r rune) bool { return !unicode.IsLetter(r) })
	fmt.Println(reflect.TypeOf(words))
	fmt.Printf("%T", words)
	fmt.Println(words)
	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	fmt.Println(kva)
}
func main() {
	splitContentsIntoAnArrayOfWords()
}
