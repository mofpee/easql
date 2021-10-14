package easql

import (
	"database/sql"

	"github.com/Masterminds/squirrel"
)

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
