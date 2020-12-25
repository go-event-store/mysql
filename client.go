package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Client struct {
	db *sql.DB
}

func (c *Client) Conn() interface{} {
	return c.db
}

func (c *Client) Exists(ctx context.Context, collection string) (bool, error) {
	err := c.db.QueryRowContext(ctx, fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", collection)).Err()
	if err != nil && strings.Contains(err.Error(), "Error 1146") {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *Client) Delete(ctx context.Context, collection string) error {
	_, err := c.db.ExecContext(ctx, "DROP TABLE IF EXISTS `"+collection+"`;")

	return err
}

func (c *Client) Reset(ctx context.Context, collection string) error {
	_, err := c.db.ExecContext(ctx, "TRUNCATE TABLE `"+collection+"`;")

	return err
}

func (c *Client) Insert(ctx context.Context, collection string, values map[string]interface{}) error {
	columns := make([]string, 0, len(values))
	placeholder := make([]string, 0, len(values))
	parameters := make([]interface{}, 0, len(values))

	for column, parameter := range values {
		columns = append(columns, column)
		parameters = append(parameters, parameter)
		placeholder = append(placeholder, "?")
	}

	_, err := c.db.ExecContext(
		ctx,
		"INSERT INTO `"+collection+"` ("+strings.Join(columns, ",")+") VALUES ("+strings.Join(placeholder, ",")+");",
		parameters...,
	)

	return err
}

func (c *Client) Remove(ctx context.Context, collection string, identifiers map[string]interface{}) error {
	conditions := make([]string, 0, len(identifiers))
	parameters := make([]interface{}, 0, len(identifiers))

	counter := 0

	for column, parameter := range identifiers {
		counter++

		parameters = append(parameters, parameter)
		conditions = append(conditions, column+` = ?`)
	}

	_, err := c.db.ExecContext(
		ctx,
		"DELETE FROM `"+collection+"` WHERE "+strings.Join(conditions, " AND ")+";",
		parameters...,
	)

	return err
}

func (c *Client) Update(ctx context.Context, collection string, values map[string]interface{}, identifiers map[string]interface{}) error {
	conditions := make([]string, 0, len(identifiers))
	updates := make([]string, 0, len(values))
	parameters := make([]interface{}, 0, len(identifiers)+len(values))

	for column, parameter := range values {
		parameters = append(parameters, parameter)
		updates = append(updates, "`"+column+"` = ?")
	}

	for column, parameter := range identifiers {
		parameters = append(parameters, parameter)
		conditions = append(conditions, "`"+column+"` = ?")
	}

	_, err := c.db.ExecContext(
		ctx,
		"UPDATE `"+collection+"` SET "+strings.Join(updates, ",")+" WHERE "+strings.Join(conditions, " AND ")+";",
		parameters...,
	)

	return err
}

func NewClient(db *sql.DB) *Client {
	return &Client{db: db}
}
