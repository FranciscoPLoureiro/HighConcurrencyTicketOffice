package purchase

import (
	"errors"
	"fmt"
)

// Fault names a point in a sale where the process can be made to die.
//
// The brief asks for controlled fault injection that kills the process between
// the decrement and the publish, because a recovery mechanism nobody has
// watched work is a recovery mechanism nobody should believe. These are the two
// points worth killing at — the two windows in which a ticket can go missing.
type Fault string

const (
	// FaultAfterDecrement abandons the sale once Redis has granted the ticket
	// and before PostgreSQL has heard of it. The ticket is out of circulation
	// and no record of it exists anywhere durable.
	FaultAfterDecrement Fault = "after-decrement"

	// FaultAfterRecord abandons it once the row is written and before the
	// broker has the message. The seat is taken and nothing will fulfil it.
	FaultAfterRecord Fault = "after-record"
)

// ErrFaultInjected is what an abandoned sale returns.
//
// A distinct error because it must never be mistaken for one of the ordinary
// failures: the compensation paths exist to tidy up after those, and tidying up
// here would defeat the entire point. The whole value of the injected fault is
// that nothing cleans up after it.
var ErrFaultInjected = errors.New("fault injected deliberately")

// Faults is the set of armed failure points.
//
// The zero value arms nothing, which is what every environment except a
// deliberate demonstration runs with.
type Faults struct {
	// Armed is the point to fail at, or empty.
	Armed Fault

	// Kill, when set, is called instead of returning an error — and is where
	// os.Exit lives in the compose demonstration.
	//
	// Two modes because the brief asks for a killed *process* and a test
	// cannot assert anything about a process that has called os.Exit. What
	// both modes share is the only property that matters: the sale stops
	// exactly there, having compensated nothing, which is precisely the state
	// a crash leaves behind. Returning an error simulates the crash rather
	// than performing one, and the sweeper cannot tell the difference because
	// there is no difference in what it can see.
	Kill func()
}

// after abandons the sale if this is the armed point.
func (f Faults) after(point Fault) error {
	if f.Armed == "" || f.Armed != point {
		return nil
	}

	if f.Kill != nil {
		f.Kill()
	}

	return fmt.Errorf("%w at %s", ErrFaultInjected, point)
}

// ParseFault turns configuration into an armed point, rejecting anything it
// does not recognise.
//
// Rejected rather than ignored, and loudly: a typo in this variable would
// silently disarm a demonstration whose entire purpose is to fail, and the
// operator would watch a system quietly succeed and conclude the recovery works.
func ParseFault(raw string) (Fault, error) {
	switch Fault(raw) {
	case "":
		return "", nil
	case FaultAfterDecrement:
		return FaultAfterDecrement, nil
	case FaultAfterRecord:
		return FaultAfterRecord, nil
	default:
		return "", fmt.Errorf("unknown fault %q, want %q or %q",
			raw, FaultAfterDecrement, FaultAfterRecord)
	}
}
