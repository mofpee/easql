package easql

import (
	"database/sql"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

type Queryer interface {
	Get(interface{}, squirrel.SelectBuilder) error
	Select(interface{}, squirrel.SelectBuilder) error
	Insert(squirrel.InsertBuilder) (sql.Result, error)
	Update(squirrel.UpdateBuilder) (sql.Result, error)
	Delete(squirrel.DeleteBuilder) (sql.Result, error)
}

type RawQueryer interface {
	Get(interface{}, string, ...interface{}) error
	Select(interface{}, string, ...interface{}) error
	Exec(string, ...interface{}) (sql.Result, error)
}

type queryer struct {
	raw RawQueryer
}

func (q *queryer) Get(v interface{}, b squirrel.SelectBuilder) error {
	query, args, err := b.ToSql()
	if err != nil {
		return err
	}
	return q.raw.Get(v, query, args...)
}

func (q *queryer) Select(v interface{}, b squirrel.SelectBuilder) error {
	query, args, err := b.ToSql()
	if err != nil {
		return err
	}
	return q.raw.Select(v, query, args...)
}

type queryBuilder interface {
	ToSql() (string, []interface{}, error)
}

func (q *queryer) execQuery(builder queryBuilder) (sql.Result, error) {
	query, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}
	return q.raw.Exec(query, args...)
}

func (q *queryer) Insert(b squirrel.InsertBuilder) (sql.Result, error) {
	return q.execQuery(b)
}

func (q *queryer) Update(b squirrel.UpdateBuilder) (sql.Result, error) {
	return q.execQuery(b)
}

func (q *queryer) Delete(b squirrel.DeleteBuilder) (sql.Result, error) {
	return q.execQuery(b)
}

type DB struct {
	raw *sqlx.DB
	Queryer
}

func NewDB(raw *sqlx.DB) *DB {
	return &DB{
		raw:     raw,
		Queryer: &queryer{raw: raw},
	}
}

func (db *DB) Close() error {
	return db.raw.Close()
}

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
