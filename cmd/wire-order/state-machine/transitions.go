// Package statemachine defines the order lifecycle state machine for the wire-order service.
// Valid transitions are restricted to prevent invalid state progression through a compile-time-safe map.
package statemachine

import (
	"fmt"
)

// OrderStatus represents the possible states an order can be in during its lifecycle.
type OrderStatus string

const (
	// CREATED is the initial state when an order is first persisted to the database.
	CREATED OrderStatus = "CREATED"

	// PENDING_PAYMENT indicates payment has not yet been received.
	PENDING_PAYMENT OrderStatus = "PENDING_PAYMENT"

	// PAYMENT_RECEIVED confirms that payment has been successfully processed.
	PAYMENT_RECEIVED OrderStatus = "PAYMENT_RECEIVED"

	// INVENTORY_RESERVED means inventory has been tentatively allocated for this order.
	INVENTORY_RESERVED OrderStatus = "INVENTORY_RESERVED"

	// PROCESSING indicates the order is being prepared (e.g., picking, packing).
	PROCESSING OrderStatus = "PROCESSING"

	// FULFILLED means the order has been completed and shipped/delivered.
	FULFILLED OrderStatus = "FULFILLED"

	// CANCELLED indicates the order was cancelled before completion.
	CANCELLED OrderStatus = "CANCELLED"

	// REFUNDED indicates payment was refunded after cancellation.
	REFUNDED OrderStatus = "REFUNDED"
)

// IsValid checks if an order status string is a known valid state.
func IsValid(s string) bool {
	_, ok := allStatuses[OrderStatus(s)]
	return ok
}

// IsTerminal returns true if the status represents a final state (no further transitions).
func IsTerminal(s OrderStatus) bool {
	switch s {
	case FULFILLED, CANCELLED, REFUNDED:
		return true
	default:
		return false
	}
}

// allStatuses is a set of all valid order statuses.
var allStatuses = map[OrderStatus]bool{
	CREATED:           true,
	PENDING_PAYMENT:   true,
	PAYMENT_RECEIVED:  true,
	INVENTORY_RESERVED: true,
	PROCESSING:        true,
	FULFILLED:         true,
	CANCELLED:         true,
	REFUNDED:          true,
}

// ValidTransitions maps each order status to the list of statuses it can transition to.
// Only transitions explicitly listed here are allowed; all others will be rejected.
var ValidTransitions = map[OrderStatus][]OrderStatus{
	CREATED: {
		PENDING_PAYMENT, CANCELLED, REFUNDED,
	},
	PENDING_PAYMENT: {
		PAYMENT_RECEIVED, CANCELLED, REFUNDED,
	},
	PAYMENT_RECEIVED: {
		INVENTORY_RESERVED, CANCELLED, REFUNDED,
	},
	INVENTORY_RESERVED: {
		PROCESSING, CANCELLED, REFUNDED,
	},
	PROCESSING: {
		FULFILLED, CANCELLED, REFUNDED,
	},
	FULFILLED: {
		// Terminal state — no outgoing transitions allowed.
	},
	CANCELLED: {
		REFUNDED, // Cancelled orders can be refunded as a follow-up step.
	},
	REFUNDED: {
		// Terminal state — refund is the final outcome.
	},
}

// TransitionResult captures the outcome of an attempted state transition.
type TransitionResult struct {
	Valid      bool
	From       OrderStatus
	To         OrderStatus
	CanReach   []OrderStatus
}

// CanTransition checks whether a transition from `from` to `to` is valid per the state machine rules.
func CanTransition(from, to OrderStatus) bool {
	validTargets, ok := ValidTransitions[from]
	if !ok {
		return false
	}
	for _, target := range validTargets {
		if target == to {
			return true
		}
	}
	return false
}

// AllowedNextStates returns the slice of statuses that may follow `from`.
// Returns an empty slice (nil) if the status is terminal or unknown.
func AllowedNextStates(from OrderStatus) []OrderStatus {
	return ValidTransitions[from]
}

// ValidateTransition performs a full transition validation and returns a descriptive error if invalid.
// It checks both source validity, target membership in allowed next states, and terminal-state guards.
func ValidateTransition(from, to OrderStatus) (TransitionResult, error) {
	if !IsValid(string(from)) {
		return TransitionResult{}, fmt.Errorf("statemachine: invalid source status %q", string(from))
	}
	if !IsValid(string(to)) {
		return TransitionResult{From: from}, fmt.Errorf("statemachine: invalid target status %q", string(to))
	}

	result := TransitionResult{
		Valid:    false,
		From:     from,
		To:       to,
		CanReach: AllowedNextStates(from),
	}

	if !CanTransition(from, to) {
		return result, fmt.Errorf(
			"statemachine: invalid transition from %q to %q (allowed next states: %v)",
			string(from), string(to), formatStatuses(result.CanReach),
		)
	}

	result.Valid = true
	return result, nil
}

// formatStatuses converts a slice of OrderStatus into a human-readable comma-separated string.
func formatStatuses(statuses []OrderStatus) string {
	out := make([]string, len(statuses))
	for i, s := range statuses {
		out[i] = string(s)
	}
	return fmt.Sprintf("[%s]", joinStrings(out))
}

// joinStrings joins strings with commas. A small inline helper to avoid importing "strings" in tight loops.
func joinStrings(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	result := ss[0]
	for i := 1; i < len(ss); i++ {
		result += ", " + ss[i]
	}
	return result
}
