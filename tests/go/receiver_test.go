package integration_test

import "io"

type MockReceiver[T any] struct {
	ZeroChunk T
	Chunks    []T
}

func (m *MockReceiver[T]) Receive() (T, error) {
	if len(m.Chunks) == 0 {
		return m.ZeroChunk, io.EOF
	}

	next := m.Chunks[0]
	m.Chunks = m.Chunks[1:]
	return next, nil
}

func (m *MockReceiver[T]) IsComplete() bool {
	return len(m.Chunks) == 0
}
