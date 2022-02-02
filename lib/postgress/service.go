package postgress

import (
	"database/sql"
)

type Service interface {
	DeQueue(int) ([]QueueValue, error)
	Queue(QueueValue) error
}

type QueueValue struct {
	Key   string
	Value string
}

type service struct {
	db *sql.DB
}

var _ Service = &service{}

func NewService(db *sql.DB) Service {
	return &service{
		db: db,
	}
}

func (s *service) DeQueue(nbr int) ([]QueueValue, error) {
	return nil, nil
}

func (s *service) Queue(value QueueValue) error {
	return nil
}
