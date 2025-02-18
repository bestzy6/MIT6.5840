package kvraft

type Database struct {
	Me int
	DB map[string]string
}

func NewDatabase(me int) *Database {
	return &Database{
		Me: me,
		DB: make(map[string]string),
	}
}

func (s *Database) Get(key string) (string, Err) {
	if val, ok := s.DB[key]; ok {
		DPrintf("[Database%d] Get key:[%s] success, val:[%s]", s.Me, key, val)
		return val, OK
	}
	return "", ErrNoKey
}

func (s *Database) Put(key string, value string) (string, Err) {
	s.DB[key] = value
	DPrintf("[Database%d] Put key:[%s] success, val:[%s]", s.Me, key, value)
	return value, OK
}

func (s *Database) Append(key string, value string) (string, Err) {
	if val, ok := s.DB[key]; ok {
		s.DB[key] = val + value
	} else {
		s.DB[key] = value
	}
	DPrintf("[Database%d] Append key:[%s] success, val:[%s]", s.Me, key, s.DB[key])
	return s.DB[key], OK
}
