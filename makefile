postgres_user := postgres
password := mypass
database := postgres
port := 5432
host := 0.0.0.0



gen: 
	buf generate

postgres: 
	docker run --rm -d --name postgres -p 5432:5432 -e POSTGRES_USER=$(postgres_user) -e POSTGRES_PASSWORD=$(password) -e POSTGRES_DB=$(database) postgres:13

migrateup: 
	migrate -path db/migration -database "postgresql://$(postgres_user):$(password)@localhost:5432/$(database)?sslmode=disable" -verbose up

migratedown: 
	migrate -path db/migration -database "postgresql://$(postgres_user):$(password)@localhost:5432/$(database)?sslmode=disable" -verbose down -all 

test: 
	(HOST=$(host) PORT=$(port) USER=$(postgres_user) PASSWORD=$(password) DBNAME=$(database) go test -v -count=1 ./...)

psqlconnect: 
	psql -h 0.0.0.0 -p 5432 -U postgres -d postgres -W password 
