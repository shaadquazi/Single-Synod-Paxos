NO_OF_PROPOSERS = 2
TOLERANCE = 1

# The system requires 2n+1 servers to tolerate the failure of n servers.
NO_OF_ACCEPTORS = 2 * TOLERANCE + 1
MAJORITY = (NO_OF_ACCEPTORS // 2) + 1
MAX_DURATION = NO_OF_ACCEPTORS * 30

CONSENSUS_REACHED = []
EVENTS = []