package banking_kata

import (
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAccount_printStatement(t *testing.T) {
	a := Account{}

	assert.Equal(t, a.PrintStatement(), "")
}

func TestAccount_Deposit(t *testing.T) {
	bank := NewBank(eventstore.NewInMemoryEventStore[AccountEvent]())

	t.Run("deposit 1", func(t *testing.T) {
		a := bank.NewAccount()

		time.Sleep(10 * time.Millisecond)

		a.Deposit(1)

		assert.Eventually(t, func() bool { return a.PrintStatement() == "Amount Balance\n+1 1" }, time.Second, time.Millisecond)
	})

	t.Run("deposit twice", func(t *testing.T) {
		a := bank.NewAccount()

		time.Sleep(10 * time.Millisecond)

		a.Deposit(1)
		a.Deposit(1)

		assert.Eventually(t, func() bool { return a.PrintStatement() == "Amount Balance\n+1 1\n+1 2" }, time.Second, time.Millisecond)
	})

	t.Run("deposit and withdraw", func(t *testing.T) {
		a := bank.NewAccount()

		time.Sleep(10 * time.Millisecond)

		a.Deposit(1)
		a.Withdraw(1)

		assert.Eventually(t, func() bool { return a.PrintStatement() == "Amount Balance\n+1 1\n-1 0" }, time.Second, time.Millisecond)
	})
}
