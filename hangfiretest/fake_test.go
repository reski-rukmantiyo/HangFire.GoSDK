package hangfiretest_test

import (
	"context"
	"errors"
	"testing"

	"hangfire-gosdk/hangfire"
	"hangfire-gosdk/hangfiretest"
)

func TestFakeClientRecordsCalls(t *testing.T) {
	ctx := context.Background()
	var c hangfire.Client = hangfiretest.NewFakeClient()
	inv := hangfire.Invocation{Type: "App.Mailer, App", Method: "Send", ParameterTypes: []string{"System.String"}, Args: []any{"a@b.c"}}
	id, err := c.Enqueue(ctx, inv, hangfire.WithQueue("mail"))
	if err != nil {
		t.Fatal(err)
	}
	child, err := c.ContinueJobWith(ctx, id, inv, nil, hangfire.OnlyOnSucceededState)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Delete(ctx, child); !ok {
		t.Error("Delete of a created job returned false")
	}
	if ok, _ := c.Delete(ctx, "99"); ok {
		t.Error("Delete of an unknown job returned true")
	}
	if _, err := c.Enqueue(ctx, inv, hangfire.WithQueue("Bad Queue")); !errors.Is(err, hangfire.ErrInvalidQueue) {
		t.Errorf("invalid queue: %v", err)
	}

	f := c.(*hangfiretest.FakeClient)
	created := f.Created()
	if len(created) != 2 || created[0].Options.Queue != "mail" || created[0].Invocation.Args[0] != "a@b.c" {
		t.Fatalf("created = %+v", created)
	}
	if aw, ok := created[1].State.(hangfire.Awaiting); !ok || aw.ParentID != id {
		t.Errorf("continuation state = %+v", created[1].State)
	}
	if calls := f.Calls(); len(calls) != 4 || calls[2].Method != "Delete" || calls[2].JobID != child {
		t.Errorf("calls = %+v", calls)
	}
}
