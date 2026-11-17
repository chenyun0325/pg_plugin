package pgmapping

// ExampleEngine merges legacy.orders_0/orders_1 into a unified table and splits
// app.orders into horizontal shards and a separate detail table in the same DB.
func ExampleEngine() *Engine {
	base := map[string]Column{"order_id": {Source: "id"}, "tenant_id": {Source: "tenant_id"}}
	var routes []Route
	for _, src := range []string{"legacy.orders_0", "legacy.orders_1"} {
		routes = append(routes, Route{ID: "merge/" + src, SourceTable: src, TargetTable: "app.orders", Columns: map[string]Column{"order_id": {Source: "id"}, "tenant_id": {Source: "tenant_id"}, "customer_name": {Source: "customer"}}})
	}
	for i, target := range []string{"legacy.orders_0", "legacy.orders_1"} {
		cols := map[string]Column{}
		for k, v := range base {
			cols[k] = v
		}
		cols["customer_name"] = Column{Source: "payload", Calls: []Call{{Name: "json_get", Args: []string{"customer", "name"}}, {Name: "text"}}}
		routes = append(routes, Route{ID: target, SourceTable: "app.orders", TargetTable: target, Columns: cols, Shard: &Shard{Column: "tenant_id", Modulus: 2, Remainder: int64(i)}})
	}
	routes = append(routes, Route{ID: "detail", SourceTable: "app.orders", TargetTable: "app.order_detail", Columns: map[string]Column{"order_id": {Source: "id"}, "note": {Source: "payload", Calls: []Call{{Name: "json_get", Args: []string{"note"}}}}}})
	e, err := NewEngine("v1", routes)
	if err != nil {
		panic(err)
	}
	return e
}
