package main

type Message interface{}

type Notifier interface {
	Notify(msg Message) error
	NotifierName() string
	Ignore(msg Message) bool
}
