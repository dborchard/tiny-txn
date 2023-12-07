package client

var _ Client = new(DB)

type DB struct {
}

func (db *DB) NewTransaction() Transaction {
	//TODO implement me
	panic("implement me")
}

func (db *DB) GetLatestCommitTS() uint64 {
	//TODO implement me
	panic("implement me")
}

func (db *DB) Close() error {
	//TODO implement me
	panic("implement me")
}
