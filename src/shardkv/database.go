package shardkv

type Database struct {
	GID int
	Me  int
	DB  map[string]string
}

func NewDatabase(GID, me int) *Database {
	return &Database{
		GID: GID,
		Me:  me,
		DB:  make(map[string]string),
	}
}

func (s *Database) Get(key string) (string, Err) {
	if val, ok := s.DB[key]; ok {
		DPrintf("[Database%d-%d] Get key:[%s] success,Shard:[%d] val:[%s]", s.GID, s.Me, key, key2shard(key), val)
		return val, OK
	}
	return "", ErrNoKey
}

func (s *Database) Put(key string, value string) (string, Err) {
	s.DB[key] = value
	DPrintf("[Database%d-%d] Put key:[%s] success,Shard:[%d] val:[%s]", s.GID, s.Me, key, key2shard(key), value)
	return value, OK
}

func (s *Database) Append(key string, value string) (string, Err) {
	val, ok := s.DB[key]
	if ok {
		s.DB[key] = val + value
	} else {
		s.DB[key] = value
	}
	DPrintf("[Database%d-%d] Append key:[%s] success,Shard:[%d], orgin_val:[%s], val:[%s]", s.GID, s.Me, key, key2shard(key), val, s.DB[key])
	return s.DB[key], OK
}
