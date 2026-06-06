package envelope

// Typed result enums. Every operation returns an explicit typed state —
// never a fake-success payload, never a boolean that conflates "lost the
// race" with "transport error" (audit Avoid #2/#4).

// ClaimResult is the outcome of a CAS claim attempt.
type ClaimResult string

const (
	ClaimClaimed ClaimResult = "claimed" // this caller won the claim
	ClaimLost    ClaimResult = "lost"    // another caller legitimately won
	ClaimError   ClaimResult = "error"   // transport/store failure; retryable
)

// AskResult is the outcome of polling an ask ticket.
type AskResult string

const (
	AskAnswered     AskResult = "answered"
	AskPending      AskResult = "pending"
	AskTimedOut     AskResult = "timed_out"
	AskExpired      AskResult = "expired"
	AskNoSuchTicket AskResult = "no_such_ticket"
)
