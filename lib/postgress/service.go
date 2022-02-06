package postgress

import (
	"context"
	"database/sql"
	"fmt"
)

type Service interface {
	DeQueue(context.Context, int) ([]QueueValue, error)
	Queue(context.Context, QueueValue) error
	DeQueueLock(context.Context, int) ([]QueueValue, error)
	GetTx(context.Context) (*sql.Tx, error)
	CommitTx(*sql.Tx) error
	RollBack(*sql.Tx) error
	DeQueueLockTx(context.Context, *sql.Tx, int) ([]QueueValue, error) // Update to not send the database tx out here, make it private instead using a struct
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
	query := fmt.Sprintf("INSERT INTO events (value) VALUES ('%s')", value.Value)

	_, err = tx.ExecContext(ctx, query)
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
	query := fmt.Sprintf(`
		DELETE FROM
			events
		USING (
			SELECT * FROM events LIMIT %d FOR UPDATE SKIP LOCKED
		) q
		WHERE q.key = events.key RETURNING events.*`, nbr)
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
	for i := 0; i < nbr && res.Next(); i++ { // This is needed since there will mostly allways be more rows to fetch and limit will not help here ...
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

func (s *service) GetTx(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (s *service) CommitTx(tx *sql.Tx) error {
	return tx.Commit()
}

func (s *service) RollBack(tx *sql.Tx) error {
	return tx.Rollback()
}

func (s *service) DeQueueLockTx(ctx context.Context, tx *sql.Tx, nbr int) ([]QueueValue, error) {

	// We can remove the transaction within here
	// since we will handle that with golang.

	// Need to figure out how to tell it when to commit the lock
	// have to use some type of channel and go routine for it I guess or
	// it have to be moved back to the grcp service so it can hold the lock in there.
	// Think I will stick with creating a new contect with Done and then
	// check the done for releasing the lock.

	// Split the delete statement out
	// We controll our stuff as two thing
	// One select query
	// and one delete query
	// will still be in the same transaction.
	// Pass in the ids that we wish to delete
	// This should be fine, continue tomorrow with it.

	dequeueQuery := fmt.Sprintf("SELECT * FROM events LIMIT %d FOR UPDATE SKIP LOCKED", nbr)
	res, err := tx.QueryContext(ctx, dequeueQuery)

	if err != nil {
		return nil, err
	}

	out := []QueueValue{}
	ids := []string{}
	for res.Next() {
		var val QueueValue
		err := res.Scan(&val.Key, &val.Value)
		if err != nil {
			return nil, err
		}
		ids = append(ids, val.Key)
		out = append(out, val)
	}

	in := "("
	for i, id := range ids {
		in = in + id
		if i < len(ids)-1 {
			in = in + ","
		}
	}
	in = in + ")"

	delQuery := fmt.Sprintf(`
	DELETE FROM
		events
	WHERE events.key IN %s`, in)
	_, err = tx.ExecContext(ctx, delQuery)
	if err != nil {
		return nil, err
	}

	return out, nil
}
