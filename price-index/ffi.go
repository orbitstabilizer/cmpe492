package main

import (
	"os"
	"syscall"
	"unsafe"
)



type SHMReader[T any] struct {
	file       *os.File
	mappedData []byte
	Data       *T
}

type SHMWriter[T any] struct {
	file       *os.File
	mappedData []byte
	Data       *T
}


func NewSHMReader[T any](fname string) (*SHMReader[T], error) {
	var v T
	size := int(unsafe.Sizeof(v))

	reader := SHMReader[T]{}

	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	reader.file = file

	data, err := syscall.Mmap(int(file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}
	reader.mappedData = data

	reader.Data = (*T)(unsafe.Pointer(&data[0]))
	return &reader, nil
}

func (r *SHMReader[T]) Close() {
	if r.file != nil {
		r.file.Close()
	}
	if r.mappedData != nil {
		syscall.Munmap(r.mappedData)
	}
}

func NewSHMWriter[T any](fname string) (*SHMWriter[T], error) {
	var v T
	size := int(unsafe.Sizeof(v))

	writer := SHMWriter[T]{}

	file, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	writer.file = file

	err = file.Truncate(int64(size))
	if err != nil {
		file.Close()
		return nil, err
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}

	writer.Data = (*T)(unsafe.Pointer(&data[0]))
	return &writer, nil
}

func (r *SHMWriter[T]) Close() {
	if r.file != nil {
		r.file.Close()
	}
	if r.mappedData != nil {
		syscall.Munmap(r.mappedData)
	}
}


