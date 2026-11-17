"""RTP jitter buffer and playout scheduler (starter code with known defects)."""

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
        self.stats = {"received": 0, "duplicate": 0, "late": 0,
                      "plc": 0, "evicted": 0, "fec": 0, "retransmit": 0}

    # -- adaptive depth ------------------------------------------------
    def update_depth(self, jitter_ms):
        self.depth = self.min_depth
        return self.depth

    # -- ingest ---------------------------------------------------------
    def push(self, seq, payload=b"", arrival=0.0, expected=0.0, kind="media"):
        self.stats["received"] += 1
        self.buf.append({"seq": seq, "payload": payload, "arrival": arrival,
                         "expected": expected, "kind": kind})
        return True

    # -- playout --------------------------------------------------------
    def drain(self, now):
        events = []
        while self.buf:
            item = self.buf.pop(0)
            events.append({"seq": item["seq"], "payload": item["payload"],
                           "plc": False, "kind": item["kind"]})
        return events

    def flush(self):
        self.buf.clear()

    # -- misc -----------------------------------------------------------
    def set_skew(self, seconds):
        self.skew = seconds

    def loss_rate(self):
        total = self.stats["received"] + self.stats["plc"]
        if total == 0:
            return 0.0
        return self.stats["plc"] / total
