package example

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-event-store/eventstore"
	"github.com/go-event-store/mysql"
)

func CreateReadModelProjection(ctx context.Context, pool *sql.DB) {
	typeRegistry := eventstore.NewTypeRegistry()
	typeRegistry.RegisterAggregate(FooAggregate{})
	typeRegistry.RegisterEvents(FooEvent{}, BarEvent{})

	ps := mysql.NewPersistenceStrategy(pool)
	es := eventstore.NewEventStore(ps)
	pm := mysql.NewProjectionManager(pool)

	client := mysql.NewClient(pool)

	rm := NewFooReadModel(client)

	projector := eventstore.NewReadModelProjector("foo_read_model_projection", rm, es, pm)
	err := projector.
		FromStream(FooStream, []eventstore.MetadataMatch{}).
		Init(func() interface{} {
			return struct{}{}
		}).
		When(map[string]eventstore.EventHandler{
			FooEventName: func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				projector.ReadModel.Stack(
					"insert",
					map[string]interface{}{
						"id":           event.UUID().String(),
						"aggregate_id": event.AggregateID().String(),
						"value":        event.Payload().(FooEvent).Foo,
					},
				)

				projector.ReadModel.Stack(
					"update",
					map[string]interface{}{
						"value": event.Payload().(FooEvent).Foo,
					},
					map[string]interface{}{
						"aggregate_id": event.AggregateID().String(),
					},
				)

				return state, nil
			},
			BarEventName: func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				projector.ReadModel.Stack(
					"insert",
					map[string]interface{}{
						"id":           event.UUID().String(),
						"aggregate_id": event.AggregateID().String(),
						"value":        event.Payload().(BarEvent).Bar,
					},
				)

				projector.ReadModel.Stack(
					"remove",
					map[string]interface{}{
						"aggregate_id": event.AggregateID().String(),
					},
				)

				return state, nil
			},
		}).
		Run(ctx, false)

	if err != nil {
		fmt.Println(err)
	}
}
