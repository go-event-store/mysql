package mysql_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	mysql "github.com/go-event-store/mysql"
	_ "github.com/go-sql-driver/mysql"
)

func Test_MysqlClient(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("mysql", "user:password@/event-store?parseTime=true")
	if err != nil {
		t.Error(err)
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	client := mysql.NewClient(db)

	t.Run("Table Handling", func(t *testing.T) {
		exists, err := client.Exists(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}

		if exists {
			t.Fatal("unexisting table should not be found")
		}

		_, err = client.Conn().(*sql.DB).ExecContext(ctx, "CREATE TABLE test (name VARCHAR(150) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")
		if err != nil {
			t.Fatal(err)
		}

		exists, err = client.Exists(ctx, "test")
		if !exists {
			t.Fatal("existing table should be found")
		}

		err = client.Delete(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}

		exists, err = client.Exists(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("delete table should not be found")
		}
	})

	t.Run("Table Insert Item", func(t *testing.T) {
		_, err := client.Conn().(*sql.DB).ExecContext(ctx, `
			CREATE TABLE insert_test (
				no INT NOT NULL AUTO_INCREMENT,
				name VARCHAR(150) NOT NULL,
				PRIMARY KEY (no)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)
		if err != nil {
			t.Fatal(err)
		}

		err = client.Insert(ctx, "insert_test", map[string]interface{}{"name": "Rudi"})
		if err != nil {
			t.Fatal(err)
		}

		var no int
		var name string

		err = client.Conn().(*sql.DB).QueryRowContext(ctx, "SELECT * FROM insert_test").Scan(&no, &name)
		if err != nil {
			t.Fatal(err)
		}

		if no != 1 {
			t.Error("unexpected number value")
		}

		if name != "Rudi" {
			t.Error("unexpected name value")
		}

		err = client.Delete(ctx, "insert_test")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Table Update Item", func(t *testing.T) {
		_, err := client.Conn().(*sql.DB).ExecContext(ctx, `
			CREATE TABLE update_test (
				no INT NOT NULL AUTO_INCREMENT,
				name VARCHAR(150) NOT NULL,
				PRIMARY KEY (no)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)
		if err != nil {
			t.Fatal(err)
		}

		err = client.Insert(ctx, "update_test", map[string]interface{}{"name": "Rudi"})
		if err != nil {
			t.Fatal(err)
		}

		err = client.Update(ctx, "update_test", map[string]interface{}{"name": "Harald"}, map[string]interface{}{"no": 1})
		if err != nil {
			t.Fatal(err)
		}

		var no int
		var name string

		err = client.Conn().(*sql.DB).QueryRowContext(ctx, "SELECT * FROM update_test").Scan(&no, &name)
		if err != nil {
			t.Fatal(err)
		}

		if no != 1 {
			t.Error("unexpected number value")
		}

		if name != "Harald" {
			t.Error("unexpected name value after update")
		}

		err = client.Delete(ctx, "update_test")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Table Remove Item", func(t *testing.T) {
		_, err := client.Conn().(*sql.DB).ExecContext(ctx, `
			CREATE TABLE remove_test (
				no INT NOT NULL AUTO_INCREMENT,
				name VARCHAR(150) NOT NULL,
				PRIMARY KEY (no)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)
		if err != nil {
			t.Fatal(err)
		}

		err = client.Insert(ctx, "remove_test", map[string]interface{}{"name": "Rudi"})
		if err != nil {
			t.Fatal(err)
		}

		err = client.Remove(ctx, "remove_test", map[string]interface{}{"no": 1})
		if err != nil {
			t.Fatal(err)
		}

		err = client.Conn().(*sql.DB).QueryRowContext(ctx, "SELECT * FROM remove_test").Scan()
		if err != sql.ErrNoRows {
			t.Error("No Item should be found")
		}

		err = client.Delete(ctx, "remove_test")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Table Reset", func(t *testing.T) {
		_, err := client.Conn().(*sql.DB).ExecContext(ctx, `
			CREATE TABLE reset_test (
				no INT NOT NULL AUTO_INCREMENT,
				name VARCHAR(150) NOT NULL,
				PRIMARY KEY (no)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)
		if err != nil {
			t.Fatal(err)
		}

		err = client.Insert(ctx, "reset_test", map[string]interface{}{"name": "Rudi"})
		if err != nil {
			t.Fatal(err)
		}

		err = client.Reset(ctx, "reset_test")
		if err != nil {
			t.Fatal(err)
		}

		err = client.Conn().(*sql.DB).QueryRowContext(ctx, "SELECT * FROM reset_test").Scan()
		if err != sql.ErrNoRows {
			t.Error("No Item should be found")
		}

		err = client.Delete(ctx, "reset_test")
		if err != nil {
			t.Fatal(err)
		}
	})
}
