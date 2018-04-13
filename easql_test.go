package easql

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	sq "gopkg.in/Masterminds/squirrel.v1"
)

func mock() (db *DB, mock sqlmock.Sqlmock, err error) {
	raw, mock, err := sqlmock.New()
	if err != nil {
		return
	}
	db = NewDB(sqlx.NewDb(raw, "mysql"))
	return
}

func TestNewDB(t *testing.T) {
	db, _, err := mock()
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)
}

func TestClose(t *testing.T) {
	db, mock, _ := mock()
	mock.ExpectClose()
	err := db.Close()
	assert.NoError(t, err)
}

func TestDBImplementsGet(t *testing.T) {
	db, _, _ := mock()
	defer db.Close()
	assert.Implements(t, (*Queryer)(nil), db)
}

func TestTxImplementsGet(t *testing.T) {
	db, mock, _ := mock()
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.Implements(t, (*Queryer)(nil), tx)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRollback(t *testing.T) {
	db, mock, _ := mock()
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, _ := db.Begin()
	assert.NoError(t, tx.Rollback())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func testQuery(queryer Queryer, fn func(Queryer)) {
	fn(queryer)
}

func TestQueryer_Get(t *testing.T) {
	fn := func(q Queryer) {
		var id int
		q.Get(&id, sq.Select("id").From("users").
			Where(sq.Eq{"id": 1}))
	}

	db, mock, _ := mock()
	defer db.Close()

	// DB
	mock.ExpectQuery("SELECT id FROM users WHERE id=?").WithArgs(1)
	testQuery(db, fn)
	assert.NoError(t, mock.ExpectationsWereMet())

	// Transaction
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM users WHERE id=?").WithArgs(1)
	mock.ExpectCommit()
	tx, _ := db.Begin()
	testQuery(tx, fn)
	tx.Commit()
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestQuery_Select(t *testing.T) {
	fn := func(q Queryer) {
		var ids []int
		q.Select(&ids, sq.Select("id").From("users"))
	}

	db, mock, _ := mock()
	defer db.Close()

	// DB
	mock.ExpectQuery("SELECT id FROM users")
	testQuery(db, fn)
	assert.NoError(t, mock.ExpectationsWereMet())

	// Transaction
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM users")
	mock.ExpectCommit()
	tx, _ := db.Begin()
	testQuery(tx, fn)
	tx.Commit()
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestQuery_Insert(t *testing.T) {
	fn := func(q Queryer) {
		q.Insert(sq.Insert("users").Columns("id").
			Values(1))
	}

	db, mock, _ := mock()
	defer db.Close()

	expectQuery := func() {
		mock.ExpectExec("INSERT INTO users").WithArgs(1)
	}

	// DB
	expectQuery()
	testQuery(db, fn)
	assert.NoError(t, mock.ExpectationsWereMet())

	// Transaction
	mock.ExpectBegin()
	expectQuery()
	tx, _ := db.Begin()
	testQuery(tx, fn)
	tx.Commit()
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestQuery_Update(t *testing.T) {
	fn := func(q Queryer) {
		q.Update(sq.Update("users").Set("name", "leo").Where(sq.Eq{"id": 1}))
	}

	db, mock, _ := mock()
	defer db.Close()

	expectQuery := func() {
		mock.ExpectExec("UPDATE users SET name").WithArgs("leo", 1)
	}

	// DB
	expectQuery()
	testQuery(db, fn)
	assert.NoError(t, mock.ExpectationsWereMet())

	// Transaction
	mock.ExpectBegin()
	expectQuery()
	tx, _ := db.Begin()
	testQuery(tx, fn)
	tx.Commit()
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestQuery_Delete(t *testing.T) {
	doQuery := func(q Queryer) {
		func(q Queryer) {
			q.Delete(sq.Delete("users").Where(sq.Eq{"id": 1}))
		}(q)
	}

	db, mock, _ := mock()
	defer db.Close()

	expectQuery := func() {
		mock.ExpectExec("DELETE FROM users").WithArgs(1)
	}

	// DB
	expectQuery()
	doQuery(db)
	assert.NoError(t, mock.ExpectationsWereMet())

	// Transaction
	mock.ExpectBegin()
	expectQuery()
	tx, _ := db.Begin()
	doQuery(tx)
	tx.Commit()
	assert.NoError(t, mock.ExpectationsWereMet())
}
