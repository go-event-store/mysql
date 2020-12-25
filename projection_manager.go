package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	eventstore "github.com/go-event-store/eventstore"
	_ "github.com/go-sql-driver/mysql"
)

type ProjectionManager struct {
	db *sql.DB
}

func (pm ProjectionManager) FetchProjectionStatus(ctx context.Context, projectionName string) (eventstore.Status, error) {
	var status eventstore.Status

	row := pm.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT status FROM %s WHERE name = ?;`, ProjectionsTable), projectionName)
	err := row.Scan(&status)

	return status, err
}

func (pm ProjectionManager) CreateProjection(ctx context.Context, projectionName string, state interface{}, status eventstore.Status) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	_, err = pm.db.ExecContext(
		ctx,
		fmt.Sprintf(`INSERT INTO %s (name, position, state, status, locked_until) VALUES (?, ?, ?, ?, NULL)`, ProjectionsTable),
		projectionName,
		"{}",
		data,
		status,
	)

	return err
}

func (pm ProjectionManager) DeleteProjection(ctx context.Context, projectionName string) error {
	r, err := pm.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE name = ?`, ProjectionsTable), projectionName)
	c, err := r.RowsAffected()
	if c == 0 {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	return err
}

func (pm ProjectionManager) ResetProjection(ctx context.Context, projectionName string, state interface{}) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	r, err := pm.db.ExecContext(
		ctx,
		fmt.Sprintf(`UPDATE %s SET status = ?, state = ?, position = ? WHERE name = ?`, ProjectionsTable),
		eventstore.StatusIdle,
		data,
		"{}",
		projectionName,
	)
	c, err := r.RowsAffected()

	if c == 0 {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	return err
}

func (pm ProjectionManager) PersistProjection(ctx context.Context, projectionName string, state interface{}, streamPositions map[string]int) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	positions, err := json.Marshal(streamPositions)
	if err != nil {
		return err
	}

	r, err := pm.db.ExecContext(
		ctx,
		fmt.Sprintf(`UPDATE %s SET status = ?, state = ?, position = ? WHERE name = ?`, ProjectionsTable),
		eventstore.StatusIdle,
		data,
		positions,
		projectionName,
	)
	c, err := r.RowsAffected()

	if c == 0 {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	return err
}

func (pm ProjectionManager) UpdateProjectionStatus(ctx context.Context, projectionName string, status eventstore.Status) error {
	r, err := pm.db.ExecContext(
		ctx,
		fmt.Sprintf(`UPDATE %s SET status = ? WHERE name = ?`, ProjectionsTable),
		status,
		projectionName,
	)
	c, err := r.RowsAffected()

	if c == 0 {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	return err
}

func (pm ProjectionManager) LoadProjection(ctx context.Context, projectionName string) (map[string]int, interface{}, error) {
	position := map[string]int{}
	var state interface{}

	row := pm.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT position, state FROM %s WHERE name = ? LIMIT 1`, ProjectionsTable), projectionName)

	var stateBytes []byte
	var positionBytes []byte

	err := row.Scan(&positionBytes, &stateBytes)
	if err == sql.ErrNoRows {
		return position, state, eventstore.ProjectionNotFound{Name: projectionName}
	}

	json.Unmarshal(stateBytes, &state)
	json.Unmarshal(positionBytes, &position)

	return position, state, err
}

func (pm ProjectionManager) ProjectionExists(ctx context.Context, projectionName string) (bool, error) {
	var name string

	row := pm.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT name FROM %s WHERE name = ?;`, ProjectionsTable), projectionName)
	err := row.Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, err
}

func NewProjectionManager(db *sql.DB) *ProjectionManager {
	return &ProjectionManager{db: db}
}
