package easql

import "github.com/jmoiron/sqlx"

type Tx struct {
	raw *sqlx.Tx
	Queryer
}

func newTx(raw *sqlx.Tx) *Tx {
	return &Tx{
		raw:     raw,
		Queryer: &queryer{raw: raw},
	}
}

func (db *DB) Begin() (*Tx, error) {
	raw, err := db.raw.Beginx()
	if err != nil {
		return nil, err
	}

	return newTx(raw), nil
}

func (tx *Tx) Commit() error {
	return tx.raw.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.raw.Rollback()
}
