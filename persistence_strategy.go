package mysql

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	eventstore "github.com/go-event-store/eventstore"
	_ "github.com/go-sql-driver/mysql"
)

const (
	EventStreamsTable = "event_streams"
	ProjectionsTable  = "projections"
)

type PersistenceStrategy struct {
	db *sql.DB
}

func GenerateTableName(streamName string) string {
	h := sha1.New()
	h.Write([]byte(streamName))

	return "_" + hex.EncodeToString(h.Sum(nil))
}

func (ps PersistenceStrategy) CreateEventStreamsTable(ctx context.Context) error {
	err := ps.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT * FROM %s LIMIT 1`, EventStreamsTable)).Err()
	if err == nil {
		return nil
	}

	_, err = ps.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
            no BIGINT(20) NOT NULL AUTO_INCREMENT,
            real_stream_name VARCHAR(150) NOT NULL,
            stream_name CHAR(41) NOT NULL,
            metadata JSON,
            PRIMARY KEY (no),
            UNIQUE KEY ix_rsn (real_stream_name)
		  ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`,
		EventStreamsTable))

	return err
}

func (ps PersistenceStrategy) CreateProjectionsTable(ctx context.Context) error {
	err := ps.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT * FROM %s LIMIT 1`, ProjectionsTable)).Err()

	if err == nil {
		return nil
	}

	_, err = ps.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
            no BIGINT(20) NOT NULL AUTO_INCREMENT,
            name VARCHAR(150) NOT NULL,
            position JSON,
            state JSON,
            status VARCHAR(28) NOT NULL,
            locked_until CHAR(26),
            PRIMARY KEY (no),
            UNIQUE KEY ix_name (name)
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`, ProjectionsTable))

	return err
}

func (ps PersistenceStrategy) AddStreamToStreamsTable(ctx context.Context, streamName string) error {
	tableName := GenerateTableName(streamName)
	stmt, err := ps.db.PrepareContext(ctx, fmt.Sprintf(`INSERT INTO %s (real_stream_name, stream_name, metadata) VALUES (?, ?, ?)`, EventStreamsTable))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(streamName, tableName, "{}")

	// @TODO check unique error
	return err
}

func (ps PersistenceStrategy) RemoveStreamFromStreamsTable(ctx context.Context, streamName string) error {
	stmt, err := ps.db.PrepareContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE real_stream_name = ?`, EventStreamsTable))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	r, err := stmt.ExecContext(ctx, streamName)
	count, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return eventstore.StreamNotFound{Stream: streamName}
	}

	return nil
}

func (ps PersistenceStrategy) FetchAllStreamNames(ctx context.Context) ([]string, error) {
	streams := []string{}

	rows, err := ps.db.QueryContext(ctx, fmt.Sprintf(`SELECT real_stream_name FROM %s WHERE real_stream_name NOT LIKE '$%%'`, EventStreamsTable))
	if err != nil {
		return streams, err
	}

	for rows.Next() {
		var stream string

		err = rows.Scan(&stream)
		if err != nil {
			return []string{}, err
		}

		streams = append(streams, stream)
	}

	return streams, nil
}

func (ps PersistenceStrategy) HasStream(ctx context.Context, streamName string) (bool, error) {
	stmt, err := ps.db.PrepareContext(ctx, fmt.Sprintf(`SELECT COUNT(real_stream_name) FROM %s WHERE real_stream_name = ?`, EventStreamsTable))
	if err != nil {
		return false, err
	}
	defer stmt.Close()

	var count int

	err = stmt.QueryRowContext(ctx, streamName).Scan(&count)
	if err != nil {
		return false, err
	}

	if count == 0 {
		return false, nil
	}

	return true, nil
}

func (ps PersistenceStrategy) DeleteStream(ctx context.Context, streamName string) error {
	err := ps.RemoveStreamFromStreamsTable(ctx, streamName)
	if err != nil {
		return err
	}

	return ps.DropSchema(ctx, streamName)
}

func (ps PersistenceStrategy) CreateSchema(ctx context.Context, streamName string) error {
	tableName := GenerateTableName(streamName)
	_, err := ps.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			no BIGINT(20) NOT NULL AUTO_INCREMENT,
			event_id CHAR(36) COLLATE utf8mb4_bin NOT NULL,
			event_name VARCHAR(100) COLLATE utf8mb4_bin NOT NULL,
			payload JSON NOT NULL,
			metadata JSON NOT NULL,
			created_at DATETIME(6) NOT NULL,
			aggregate_version INT(11) UNSIGNED GENERATED ALWAYS AS (JSON_EXTRACT(metadata, '$._aggregate_version')) STORED NOT NULL,
			aggregate_id CHAR(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(metadata, '$._aggregate_id'))) STORED NOT NULL,
			aggregate_type VARCHAR(150) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(metadata, '$._aggregate_type'))) STORED NOT NULL,
			PRIMARY KEY (no),
			UNIQUE KEY ix_event_id (event_id),
			UNIQUE KEY ix_unique_event (aggregate_type, aggregate_id, aggregate_version),
			KEY ix_query_aggregate (aggregate_type,aggregate_id,no)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`, tableName))

	return err
}

func (ps PersistenceStrategy) DropSchema(ctx context.Context, streamName string) error {
	tableName := GenerateTableName(streamName)
	_, err := ps.db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	if err != nil {
		return err
	}

	return nil
}

func (ps PersistenceStrategy) AppendTo(ctx context.Context, streamName string, events []eventstore.DomainEvent) error {
	tableName := GenerateTableName(streamName)

	tx, err := ps.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`INSERT INTO %s (event_id, event_name, payload, metadata, created_at) VALUES (?, ?, ?, ?, ?)`, tableName))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, ev := range events {
		payload, err := json.Marshal(ev.Payload())
		if err != nil {
			tx.Rollback()
			return err
		}

		metadata, err := json.Marshal(ev.Metadata())
		if err != nil {
			tx.Rollback()
			return err
		}

		_, err = stmt.ExecContext(
			ctx,
			ev.UUID().String(),
			ev.Name(),
			payload,
			metadata,
			ev.CreatedAt(),
		)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (ps PersistenceStrategy) Load(ctx context.Context, streamName string, fromNumber, count int, matcher eventstore.MetadataMatcher) (eventstore.DomainEventIterator, error) {
	query, values, err := ps.createQuery(ctx, streamName, fromNumber, matcher)
	if err != nil {
		return nil, err
	}

	return NewDomainEventIterator(ctx, ps.db, query, values, count), nil
}

func (ps PersistenceStrategy) MergeAndLoad(ctx context.Context, count int, streams ...eventstore.LoadStreamParameter) (eventstore.DomainEventIterator, error) {
	var queries []string
	var parameters []interface{}

	for _, stream := range streams {
		query, values, err := ps.createQuery(ctx, stream.StreamName, stream.FromNumber, stream.Matcher)
		if err != nil {
			return nil, err
		}

		queries = append(queries, "("+query+")")
		parameters = append(parameters, values...)
	}

	groupedQuery := queries[0]

	if len(queries) > 1 {
		groupedQuery = strings.Join(queries, " UNION ALL ") + " ORDER BY created_at ASC"
	}

	return NewDomainEventIterator(ctx, ps.db, groupedQuery, parameters, count), nil
}

func (ps PersistenceStrategy) createQuery(ctx context.Context, streamName string, fromNumber int, matcher eventstore.MetadataMatcher) (string, []interface{}, error) {
	stmt, err := ps.db.PrepareContext(ctx, fmt.Sprintf(`SELECT COUNT(stream_name) FROM %s WHERE real_stream_name = ?`, EventStreamsTable))
	if err != nil {
		return "", []interface{}{}, err
	}
	defer stmt.Close()

	var count int

	err = stmt.QueryRowContext(ctx, streamName).Scan(&count)
	if err != nil {
		return "", []interface{}{}, err
	}

	if count == 0 {
		return "", []interface{}{}, eventstore.StreamNotFound{Stream: streamName}
	}

	tableName := GenerateTableName(streamName)

	wheres, values, err := ps.createWhereClause(matcher)

	wheres = append(wheres, `no >= ?`)
	values = append(values, fromNumber)

	whereCondition := fmt.Sprintf(`WHERE %s`, strings.Join(wheres, " AND "))

	query := fmt.Sprintf(`SELECT no, event_id, event_name, payload, metadata, created_at, '%s' as stream FROM %s %s ORDER BY no ASC`, streamName, tableName, whereCondition)

	return query, values, nil
}

func (ps PersistenceStrategy) createWhereClause(matcher eventstore.MetadataMatcher) ([]string, []interface{}, error) {
	var wheres []string
	var values []interface{}

	if len(matcher) == 0 {
		return wheres, values, nil
	}

	for _, match := range matcher {
		expression := func(value string) string {
			return ""
		}

		switch match.Operation {
		case eventstore.InOperator:
			expression = func(value string) string {
				return fmt.Sprintf("IN %s", value)
			}
		case eventstore.NotInOperator:
			expression = func(value string) string {
				return fmt.Sprintf("NOT IN %s", value)
			}
		case eventstore.RegexOperator:
			expression = func(value string) string {
				return fmt.Sprintf("REGEXP %s", value)
			}
		default:
			expression = func(value string) string {
				return fmt.Sprintf("%s %s", match.Operation, value)
			}
		}

		if match.FieldType == eventstore.MetadataField {
			switch v := match.Value.(type) {
			case bool:
				wheres = append(wheres, fmt.Sprintf(`metadata->"$.%s" %s`, match.Field, expression(strconv.FormatBool(v))))
			default:
				values = append(values, match.Value)

				wheres = append(wheres, fmt.Sprintf(`JSON_UNQUOTE(metadata->>"$.%s" %s`, match.Field, expression(("?"))))
			}
		}

		if match.FieldType == eventstore.MessagePropertyField {
			switch v := match.Value.(type) {
			case bool:
				wheres = append(wheres, fmt.Sprintf(`%s %s`, match.Field, expression(strconv.FormatBool(v))))
			default:
				values = append(values, match.Value)

				wheres = append(wheres, `%s %s`, match.Field, expression(("?")))
			}
		}
	}

	return wheres, values, nil
}

func NewPersistenceStrategy(db *sql.DB) *PersistenceStrategy {
	return &PersistenceStrategy{
		db: db,
	}
}
