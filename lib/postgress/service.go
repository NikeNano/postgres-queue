package postgress

import (
	"context"
	"database/sql"
)

type Service interface {
	DeQueue(context.Context, int) ([]QueueValue, error)
	Queue(context.Context, QueueValue) error
	DeQueueLock(context.Context, int) ([]QueueValue, error)
}

type QueueValue struct {
	Key   string `db:"key"`
	Value string `db:"value"`
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

func (s *service) DeQueue(ctx context.Context, nbr int) ([]QueueValue, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT * FROM events LIMIT 1")
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	var out []QueueValue
	for rows.Next() {
		var value QueueValue
		err := rows.Scan(&value.Key, &value.Value)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		out = append(out, value)
	}

	return out, tx.Commit()
}

func (s *service) Queue(ctx context.Context, value QueueValue) error {

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO events (value) VALUES ('dog'), ('cat'), ('rabbit')")
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// This method will lock the rows so no other service or user
// can access these rows.
func (s *service) DeQueueLock(ctx context.Context, nbr int) ([]QueueValue, error) {

	// We can remove the transaction within here
	// since we will handle that with golang.

	// Need to figure out how to tell it when to commit the lock
	// have to use some type of channel and go routine for it I guess or
	// it have to be moved back to the grcp service so it can hold the lock in there.
	// Think I will stick with creating a new contect with Done and then
	// check the done for releasing the lock.
	query := `
		DELETE FROM
			events
		USING (
			SELECT * FROM events LIMIT 2 FOR UPDATE SKIP LOCKED
		) q
		WHERE q.key = events.key RETURNING events.*`
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	res, err := tx.QueryContext(ctx, query)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	out := []QueueValue{}
	for res.Next() {
		var val QueueValue
		err := res.Scan(&val.Key, &val.Value)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		out = append(out, val)
	}

	return out, tx.Commit()
}
