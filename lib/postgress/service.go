package postgress

type Service interface {
	DeQueue(int) ([]QueueValue, error)
	Queue(QueueValue) error
}

type QueueValue struct {
	Key   string
	Value string
}

type service struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}

var _ Service = &service{}

func NewService(host string, port int, user string, password string, dbname string) Service {
	return &service{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		dbname:   dbname,
	}
}

func (s *service) DeQueue(nbr int) ([]QueueValue, error) {
	return nil, nil
}

func (s *service) Queue(value QueueValue) error {
	return nil
}
