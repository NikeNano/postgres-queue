package postgress

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func dbCleanUp(db *sql.DB) error {
	_, err := db.Exec("DELETE FROM events")
	return err
}

func TestDbConnection(t *testing.T) {
	db, err := Getdb()
	require.NoError(t, err)
	err = dbCleanUp(db)
	require.NoError(t, err)
	defer db.Close()

	err = db.Ping()
	require.NoError(t, err)

}

func TestEnqueue(t *testing.T) {
	db, err := Getdb()
	require.NoError(t, err)
	err = dbCleanUp(db)
	require.NoError(t, err)
	dbsvc := NewService(db)
	err = dbsvc.Queue(context.TODO(), QueueValue{
		Value: "hello there",
	})
	require.NoError(t, err)
}

func TestDeQueue(t *testing.T) {
	db, err := Getdb()
	require.NoError(t, err)
	err = dbCleanUp(db)
	require.NoError(t, err)
	dbsvc := NewService(db)
	for _, value := range []string{"dog", "cat", "rabbit"} {
		err = dbsvc.Queue(context.TODO(), QueueValue{
			Value: value,
		})
	}
	out, err := dbsvc.DeQueue(context.TODO(), 1)
	require.NoError(t, err)
	require.Len(t, out, 1)
}

func TestDeQueueLock(t *testing.T) {
	db, err := Getdb()
	require.NoError(t, err)
	err = dbCleanUp(db)
	require.NoError(t, err)
	dbsvc := NewService(db)
	for _, value := range []string{"dog", "cat", "rabbit"} {
		err = dbsvc.Queue(context.TODO(), QueueValue{
			Value: value,
		})
	}
	out, err := dbsvc.DeQueueLock(context.TODO(), 2)
	require.NoError(t, err)
	require.Len(t, out, 2)
}

func TestDeQueueLockTx(t *testing.T) {
	db, err := Getdb()
	dbCleanUp(db)
	require.NoError(t, err)
	dbsvc := NewService(db)
	for _, value := range []string{"dog", "cat", "rabbit"} {
		err = dbsvc.Queue(context.TODO(), QueueValue{
			Value: value,
		})
	}
	require.NoError(t, err)
	ctx := context.Background()
	// Get tx first time
	tx, err := dbsvc.GetTx(ctx)
	require.NoError(t, err)
	out1, err := dbsvc.DeQueueLockTx(ctx, tx, 2)
	require.NoError(t, err)
	require.Len(t, out1, 2)
	dbsvc.RollBack(tx)
	require.NoError(t, err)
	// Get tx second time
	tx, err = dbsvc.GetTx(ctx)
	require.NoError(t, err)
	out2, err := dbsvc.DeQueueLockTx(ctx, tx, 2)
	require.NoError(t, err)
	require.Len(t, out2, 2)
	err = dbsvc.CommitTx(tx)
	require.NoError(t, err)
	require.Equal(t, out1, out2)
	// Third time make sure it is popped ...
	// Also add test where we have two transactions ...
	tx, err = dbsvc.GetTx(ctx)
	require.NoError(t, err)
	out3, err := dbsvc.DeQueueLockTx(ctx, tx, 1)
	require.NoError(t, err)
	require.Len(t, out3, 1)
	err = dbsvc.CommitTx(tx)
	require.NoError(t, err)
	require.NotEqual(t, out2[0], out3[0])
	require.Equal(t, out3[0].Value, "rabbit")

}

func TestDeQueueLockTxTwo(t *testing.T) {
	db, err := Getdb()
	dbCleanUp(db)
	require.NoError(t, err)
	dbsvc := NewService(db)
	for _, value := range []string{"dog", "cat", "snake"} {
		err = dbsvc.Queue(context.TODO(), QueueValue{
			Value: value,
		})
	}
	require.NoError(t, err)
	ctx := context.Background()
	// Get tx second time
	tx, err := dbsvc.GetTx(ctx)
	require.NoError(t, err)
	out2, err := dbsvc.DeQueueLockTx(ctx, tx, 1)
	require.NoError(t, err)
	require.Len(t, out2, 1)
	err = dbsvc.CommitTx(tx)
	require.NoError(t, err)
	// Third time make sure it is popped ...
	// Also add test where we have two transactions ...
	tx, err = dbsvc.GetTx(ctx)
	require.NoError(t, err)
	out3, err := dbsvc.DeQueueLockTx(ctx, tx, 2)
	require.NoError(t, err)
	require.Len(t, out3, 2)
	err = dbsvc.CommitTx(tx)
	require.NoError(t, err)
	require.NotEqual(t, out2[0], out3[0])
	require.Equal(t, out3[0].Value, "cat")
	require.Equal(t, out3[1].Value, "snake")

}

func TestDeQueueLockTxLock(t *testing.T) {
	db, err := Getdb()
	dbCleanUp(db)
	require.NoError(t, err)
	dbsvc := NewService(db)
	for _, value := range []string{"dog", "cat", "snake"} {
		err = dbsvc.Queue(context.TODO(), QueueValue{
			Value: value,
		})
	}
	require.NoError(t, err)
	ctx := context.Background()
	tx1, err := dbsvc.GetTx(ctx)
	require.NoError(t, err)
	out1, err := dbsvc.DeQueueLockTx(ctx, tx1, 1)
	require.NoError(t, err)
	require.Len(t, out1, 1)
	require.Equal(t, "dog", out1[0].Value)

	tx2, err := dbsvc.GetTx(ctx)
	require.NoError(t, err)
	out2, err := dbsvc.DeQueueLockTx(ctx, tx2, 1)
	require.NoError(t, err)
	require.Len(t, out2, 1)
	require.Equal(t, "cat", out2[0].Value)

	err = dbsvc.RollBack(tx1)
	require.NoError(t, err)

	tx3, err := dbsvc.GetTx(ctx)
	require.NoError(t, err)
	out3, err := dbsvc.DeQueueLockTx(ctx, tx3, 1)
	require.NoError(t, err)
	require.Len(t, out3, 1)
	require.Equal(t, out1, out3)
	require.Equal(t, "dog", out3[0].Value)
	dbsvc.CommitTx(tx1)
	dbsvc.CommitTx(tx3)
}
