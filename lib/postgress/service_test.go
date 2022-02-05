package postgress

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestDbConnection(t *testing.T) {
	host, ok := os.LookupEnv("HOST")
	if !ok {
		t.Fatal("Missing HOST env")
	}
	port, ok := os.LookupEnv("PORT")
	if !ok {
		t.Fatal("Missing PORT env")
	}

	user, ok := os.LookupEnv("USER")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	password, ok := os.LookupEnv("PASSWORD")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	dbname, ok := os.LookupEnv("DBNAME")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer db.Close()

	err = db.Ping()
	require.NoError(t, err)

}

func TestEnqueue(t *testing.T) {
	host, ok := os.LookupEnv("HOST")
	if !ok {
		t.Fatal("Missing HOST env")
	}
	port, ok := os.LookupEnv("PORT")
	if !ok {
		t.Fatal("Missing PORT env")
	}

	user, ok := os.LookupEnv("USER")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	password, ok := os.LookupEnv("PASSWORD")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	dbname, ok := os.LookupEnv("DBNAME")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	require.NoError(t, err)
	dbsvc := NewService(db)
	err = dbsvc.Queue(context.TODO(), QueueValue{
		Value: "hello there",
	})
	require.NoError(t, err)
}

func TestDeQueue(t *testing.T) {
	host, ok := os.LookupEnv("HOST")
	if !ok {
		t.Fatal("Missing HOST env")
	}
	port, ok := os.LookupEnv("PORT")
	if !ok {
		t.Fatal("Missing PORT env")
	}

	user, ok := os.LookupEnv("USER")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	password, ok := os.LookupEnv("PASSWORD")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	dbname, ok := os.LookupEnv("DBNAME")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	require.NoError(t, err)
	dbsvc := NewService(db)
	out, err := dbsvc.DeQueue(context.TODO(), 1)
	require.NoError(t, err)
	require.Len(t, out, 1)
	fmt.Println("The out value is: ", out[0].Key, out[0].Value)
}

func TestDeQueueLock(t *testing.T) {
	host, ok := os.LookupEnv("HOST")
	if !ok {
		t.Fatal("Missing HOST env")
	}
	port, ok := os.LookupEnv("PORT")
	if !ok {
		t.Fatal("Missing PORT env")
	}

	user, ok := os.LookupEnv("USER")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	password, ok := os.LookupEnv("PASSWORD")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	dbname, ok := os.LookupEnv("DBNAME")
	if !ok {
		t.Fatal("Missing HOST env")
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	require.NoError(t, err)
	dbsvc := NewService(db)
	out, err := dbsvc.DeQueueLock(context.TODO(), 1)
	require.NoError(t, err)
	require.Len(t, out, 2)
	fmt.Println("The out value is: ", out[0].Key, out[0].Value)
	fmt.Println("The out value is: ", out[1].Key, out[1].Value)
}
