package banking_kata

import (
	"context"
	"fmt"
	"github.com/beevik/guid"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
)

type Bank struct {
	eventStore *eventstore.EventStore[AccountEvent]
}

func NewBank(es *eventstore.EventStore[AccountEvent]) *Bank {
	es.WithCodec(codec.NewJSONCodecWithTypeHints[AccountEvent](map[string]codec.Unmarshaller[AccountEvent]{
		"DepositEvent": codec.UnmarshalerFunc[AccountEvent](func(payload []byte) (event AccountEvent, err error) {
			return codec.BuildJSONUnmarshalFunc[DepositEvent]()(payload)
		}),
		"WithdrawEvent": codec.UnmarshalerFunc[AccountEvent](func(payload []byte) (event AccountEvent, err error) {
			return codec.BuildJSONUnmarshalFunc[WithdrawEvent]()(payload)
		}),
	}))
	return &Bank{eventStore: es}
}

func (b Bank) NewAccount() *Account {
	return NewAccount(b.eventStore)
}

type AccountEvent interface {
	isAccountEvent()
}

type DepositEvent struct {
	Amount int
}

func (d DepositEvent) isAccountEvent() {}

type WithdrawEvent struct {
	Amount int
}

func (d WithdrawEvent) isAccountEvent() {}

type Account struct {
	accountId  string
	balance    int    // view
	statements string // view
	stream     *eventstore.Stream[AccountEvent]
}

func NewAccount(eventStore *eventstore.EventStore[AccountEvent]) *Account {
	id := guid.NewString()
	account := &Account{
		accountId: id,
		balance:   0,
		stream:    eventStore.GetStream(fmt.Sprintf("account-events-%s", id)),
	}
	account.stream.Subscribe(
		consumer.ConsumerFunc[AccountEvent](
			func(e AccountEvent) {
				switch e.(type) {
				case DepositEvent:
					amount := e.(DepositEvent).Amount
					account.balance += amount
					account.statements += "\n" + fmt.Sprintf("+%d %d", amount, account.balance)
				case WithdrawEvent:
					amount := e.(WithdrawEvent).Amount
					account.balance -= amount
					account.statements += "\n" + fmt.Sprintf("-%d %d", amount, account.balance)
				}
			},
		),
	)
	return account
}

func (a Account) PrintStatement() string {
	return a.statements
}

func (a Account) Withdraw(amount int) {
	err := a.
		stream.
		WithType("WithdrawEvent").
		Publish(context.Background(), WithdrawEvent{Amount: amount})
	if err != nil {
		panic(err)
	}
}

func (a Account) Deposit(amount int) {
	err := a.
		stream.
		WithType("DepositEvent").
		Publish(context.Background(), DepositEvent{Amount: amount})
	if err != nil {
		panic(err)
	}
}
