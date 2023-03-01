package pkafka

import "github.com/twmb/franz-go/pkg/kgo"

type persistence struct {
}

func newPersistence() (*persistence, error) {

	return &persistence{}, nil
}

func (p persistence) close() error {
	return nil
}

func (p persistence) persist(msg ...kgo.Record) error {
	return nil
}

func (p persistence) read() (<-chan []kgo.Record, error) {
	return nil, nil
}
