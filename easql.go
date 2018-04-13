package easql

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gopkg.in/Masterminds/squirrel.v1"
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

type Config struct {
	Host         string
	Port         int
	Name         string
	User         string
	Password     string
	Charset      string
	Location     string
	MaxIdleConns int
	MaxOpenConns int
	MapperFunc   func(string) string
}

func OpenMySQL(c *Config) (*DB, error) {
	sqlxdb, err := sqlx.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=%s",
		c.User,
		c.Password,
		c.Host,
		c.Port,
		c.Name,
		c.Charset,
		c.Location,
	))
	if err != nil {
		return nil, err
	}
	sqlxdb.SetMaxIdleConns(c.MaxIdleConns)
	sqlxdb.SetMaxOpenConns(c.MaxOpenConns)
	sqlxdb.MapperFunc(c.MapperFunc)

	if err := sqlxdb.Ping(); err != nil {
		return nil, err
	}

	return NewDB(sqlxdb), nil
}
