package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	eventstore "github.com/go-event-store/eventstore"
	_ "github.com/go-sql-driver/mysql"
	uuid "github.com/satori/go.uuid"
)

type DomainEventIterator struct {
	limit        int
	offset       int
	count        int
	position     int
	length       int
	done         bool
	err          error
	current      *eventstore.DomainEvent
	events       []*eventstore.DomainEvent
	db           *sql.DB
	typeRegistry eventstore.TypeRegistry
	query        string
	parameters   []interface{}
	ctx          context.Context
}

func (it *DomainEventIterator) Next() bool {
	it.position++

	if it.length >= it.position+1 {
		it.current = it.events[it.position]
		return true
	}

	it.fetchEvents()

	if it.length >= it.position+1 {
		it.current = it.events[it.position]
		return true
	}

	it.Close()

	return false
}

func (it *DomainEventIterator) Current() (*eventstore.DomainEvent, error) {
	if it.position == -1 {
		it.Next()
	}

	return it.current, it.err
}

func (it *DomainEventIterator) Rewind() {
	it.current = nil
	it.err = nil
	it.position = -1
}

func (it *DomainEventIterator) Error() error {
	return it.err
}

func (it *DomainEventIterator) Close() {
	it.current = nil
	it.events = make([]*eventstore.DomainEvent, 0)
}

func (it *DomainEventIterator) IsEmpty() (bool, error) {
	it.Next()

	return it.length == 0, it.err
}

func (it *DomainEventIterator) ToList() ([]eventstore.DomainEvent, error) {
	list := []eventstore.DomainEvent{}

	for it.Next() {
		if it.err != nil {
			return list, it.err
		}

		list = append(list, *it.current)
	}

	return list, nil
}

func (it *DomainEventIterator) fetchEvents() {
	if it.done {
		return
	}

	query := it.query
	limit := it.limit

	if it.count > 0 && it.count < it.limit {
		limit = it.count
	}

	query = fmt.Sprintf("%s LIMIT %d OFFSET %d", query, limit, it.offset)

	rows, err := it.db.QueryContext(it.ctx, query, it.parameters...)
	if err != nil {
		it.err = err
		return
	}

	counter := it.appendRows(rows)

	if counter < limit {
		it.done = true
	}

	it.count -= counter
	it.offset += counter
	it.length += counter
}

func (it *DomainEventIterator) appendRows(rows *sql.Rows) int {
	counter := 0

	for rows.Next() {
		var name, eventID, stream string
		var number int
		var payload []byte
		var createdAt time.Time
		var metadata map[string]interface{}

		var metastring []byte

		it.err = rows.Scan(&number, &eventID, &name, &payload, &metastring, &createdAt, &stream)
		if it.err != nil {
			return counter
		}

		eventType, _ := it.typeRegistry.GetTypeByName(name)

		json.Unmarshal(metastring, &metadata)

		eventValue := reflect.New(eventType)
		eventInterface := eventValue.Interface()
		it.err = json.Unmarshal(payload, eventInterface)

		metadata["stream"] = stream

		event := eventstore.
			NewDomainEvent(uuid.NewV4(), reflect.Indirect(eventValue).Interface(), metadata, createdAt).
			WithUUID(uuid.FromStringOrNil(eventID)).
			WithNumber(number)

		it.events = append(it.events, &event)

		counter++
	}

	return counter
}

func NewDomainEventIterator(ctx context.Context, db *sql.DB, query string, parameters []interface{}, count int) *DomainEventIterator {
	return &DomainEventIterator{
		limit:        1000,
		offset:       0,
		count:        count,
		position:     -1,
		length:       0,
		current:      nil,
		done:         false,
		events:       make([]*eventstore.DomainEvent, 0),
		db:           db,
		query:        query,
		parameters:   parameters,
		typeRegistry: eventstore.NewTypeRegistry(),
		ctx:          ctx,
	}
}
