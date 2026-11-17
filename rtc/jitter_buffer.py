"""RTP jitter buffer and playout scheduler.

The buffer absorbs network jitter by holding each packet until its playout
deadline.  The depth of that hold (in :data:`FRAME` units) adapts to the
measured jitter, rises immediately when the network gets worse and shrinks
one step at a time when it recovers, so playout never jumps backwards.

Packets are ordered by RTP sequence number rather than arrival order,
duplicates and packets whose deadline has already passed are discarded, and
sequence gaps are reported as packet-loss-concealment (PLC) events instead of
being silently skipped.  NACK retransmissions are inserted at their original
position, FEC packets are counted but never played, and the buffer capacity
bounds both memory use and the maximum added latency; once the playout point
falls behind, expired packets are evicted (the buffer catches up).
"""

FRAME = 0.02


class JitterBuffer:
    def __init__(self, min_depth=2, max_depth=6, jitter_gain=0.05,
                 plc_delay=0.04, capacity=8):
        self.min_depth = min_depth
        self.max_depth = max_depth
        self.jitter_gain = jitter_gain
        self.plc_delay = plc_delay
        self.capacity = capacity
        self.depth = min_depth
        self.skew = 0.0
        self.buf = []
        self.next_seq = None
        self._seqs = set()
        self._reset_stats()

    def _reset_stats(self):
        self.stats = {"received": 0, "duplicate": 0, "late": 0,
                      "plc": 0, "evicted": 0, "fec": 0, "retransmit": 0}

    # -- adaptive depth ------------------------------------------------
    def update_depth(self, jitter_ms):
        """Track ``jitter_ms`` of measured network jitter.

        Depth grows immediately (a backlog is what absorbs a jitter spike)
        but shrinks by at most one frame per update, so reducing the buffer
        never causes a discontinuous jump in the playout timeline.
        """
        target = self.min_depth + self.jitter_gain * jitter_ms
        target = max(self.min_depth, min(self.max_depth, target))
        if target > self.depth:
            self.depth = target
        elif target < self.depth:
            self.depth = max(target, self.depth - 1)
        return self.depth

    # -- timing ---------------------------------------------------------
    def _deadline(self, expected):
        """Playout deadline for a packet with nominal time ``expected``."""
        return expected + self.depth * FRAME + self.skew

    # -- ingest ---------------------------------------------------------
    def push(self, seq, payload=b"", arrival=0.0, expected=0.0, kind="media"):
        """Insert one packet and return whether it will be played."""
        self.stats["received"] += 1

        # Parity packets protect other packets; they are never rendered.
        if kind == "fec":
            self.stats["fec"] += 1
            return False

        if seq in self._seqs:
            self.stats["duplicate"] += 1
            return False

        # Anything at or before the current playout point can no longer be
        # used, even if a NACK retransmission finally delivered it.
        if self.next_seq is not None and seq < self.next_seq:
            self.stats["late"] += 1
            return False

        # A packet that arrives after its own playout deadline is too late to
        # render; concealment for that slot is emitted by drain().
        if arrival > self._deadline(expected):
            self.stats["late"] += 1
            return False

        if kind == "nack":
            self.stats["retransmit"] += 1

        if len(self.buf) >= self.capacity and seq < self.buf[0]["seq"]:
            # The new packet is older than everything buffered; keep the
            # buffer bounded by dropping it rather than evicting a newer one.
            self.stats["evicted"] += 1
            return False
        while len(self.buf) >= self.capacity:
            evicted = self.buf.pop(0)
            self._seqs.discard(evicted["seq"])
            self.stats["evicted"] += 1
            # An evicted slot is gone for good: advance the playout point so
            # it is not later mistaken for loss and concealed.
            if self.next_seq is not None and evicted["seq"] >= self.next_seq:
                self.next_seq = evicted["seq"] + 1

        self.buf.append({"seq": seq, "payload": payload, "arrival": arrival,
                         "expected": expected, "kind": kind})
        # Retransmissions can arrive out of order, so keep the queue sorted by
        # RTP sequence number rather than by arrival.
        self.buf.sort(key=lambda item: item["seq"])
        self._seqs.add(seq)
        return True

    # -- playout --------------------------------------------------------
    def drain(self, now):
        """Return every packet whose playout deadline has passed."""
        events = []
        while self.buf:
            item = self.buf[0]
            if self._deadline(item["expected"]) > now:
                break
            self.buf.pop(0)
            self._seqs.discard(item["seq"])

            # The playout point is only established once the first packet is
            # actually rendered; packets seen before that are reordering, not
            # loss, so nothing before the first played packet is concealed.
            if self.next_seq is None:
                self.next_seq = item["seq"]

            # Conceal any sequence numbers missing before this packet.  The
            # gap is older than ``item``, so its deadline has passed too.
            while self.next_seq < item["seq"]:
                events.append({"seq": self.next_seq, "payload": b"",
                               "plc": True, "kind": "plc"})
                self.stats["plc"] += 1
                self.next_seq += 1

            events.append({"seq": item["seq"], "payload": item["payload"],
                           "plc": False, "kind": item["kind"]})
            self.next_seq = item["seq"] + 1
        return events

    def flush(self):
        """Drop all buffered state, e.g. when the stream restarts."""
        self.buf.clear()
        self._seqs.clear()
        self.next_seq = None
        self.depth = self.min_depth
        self.skew = 0.0
        self._reset_stats()

    # -- misc -----------------------------------------------------------
    def set_skew(self, seconds):
        """Apply a clock-drift correction to every playout deadline."""
        self.skew = seconds

    def loss_rate(self):
        total = self.stats["received"] + self.stats["plc"]
        if total == 0:
            return 0.0
        return self.stats["plc"] / total
