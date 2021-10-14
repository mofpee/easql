package easql

import (
	"database/sql"

	"github.com/Masterminds/squirrel"
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

type queryBuilder interface {
	ToSql() (string, []interface{}, error)
}
