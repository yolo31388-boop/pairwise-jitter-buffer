import pytest

from rtc.jitter_buffer import JitterBuffer


def test_depth_grows_with_jitter():
    b = JitterBuffer()
    assert b.update_depth(40) == pytest.approx(4.0)


def test_depth_is_clamped_to_max():
    b = JitterBuffer()
    assert b.update_depth(500) == pytest.approx(6.0)


def test_depth_decreases_one_step_at_a_time():
    b = JitterBuffer()
    b.update_depth(200)
    assert b.depth == pytest.approx(6.0)
    assert b.update_depth(0) == pytest.approx(5.0)
    assert b.update_depth(0) == pytest.approx(4.0)


def test_packets_are_emitted_in_sequence_order():
    b = JitterBuffer()
    b.push(3, b"c")
    b.push(1, b"a")
    b.push(2, b"b")
    events = b.drain(now=1.0)
    assert [e["seq"] for e in events] == [1, 2, 3]


def test_duplicate_packet_is_dropped():
    b = JitterBuffer()
    b.push(1, b"a")
    b.push(1, b"a-again")
    events = b.drain(now=1.0)
    assert len(events) == 1
    assert b.stats["duplicate"] == 1


def test_late_packet_is_dropped_and_counted():
    b = JitterBuffer()
    b.push(1, b"late", arrival=0.5, expected=0.0)
    assert b.drain(now=1.0) == []
    assert b.stats["late"] == 1


def test_gap_triggers_plc_event():
    b = JitterBuffer()
    b.push(1, b"a")
    b.push(3, b"c")
    events = b.drain(now=1.0)
    assert [e["seq"] for e in events] == [1, 2, 3]
    assert events[1]["plc"] is True
    assert b.stats["plc"] == 1


def test_no_output_before_playout_deadline():
    b = JitterBuffer()
    b.push(1, b"a", arrival=0.0, expected=0.0)
    assert b.drain(now=0.01) == []


def test_retransmitted_packet_is_inserted_in_order():
    b = JitterBuffer()
    b.push(1, b"a")
    b.push(3, b"c")
    b.push(2, b"b", expected=0.0, kind="nack")
    events = b.drain(now=1.0)
    assert [e["seq"] for e in events] == [1, 2, 3]
    assert b.stats["retransmit"] == 1


def test_fec_packet_is_counted_but_not_played():
    b = JitterBuffer()
    b.push(4, b"parity", kind="fec")
    assert b.drain(now=1.0) == []
    assert b.stats["fec"] == 1


def test_capacity_evicts_lowest_sequence():
    b = JitterBuffer(capacity=4)
    for seq in range(1, 6):
        b.push(seq, b"x")
    assert b.stats["evicted"] == 1
    assert [e["seq"] for e in b.drain(now=1.0)] == [2, 3, 4, 5]


def test_flush_resets_counters():
    b = JitterBuffer()
    b.push(1, b"a")
    b.push(2, b"b")
    b.flush()
    assert b.stats["received"] == 0
    assert b.buf == []


def test_loss_rate_counts_plc_over_received():
    b = JitterBuffer()
    b.push(1, b"a")
    b.push(3, b"c")
    b.drain(now=1.0)
    assert b.loss_rate() == pytest.approx(1 / 3)


def test_skew_shifts_playout_deadline():
    b = JitterBuffer()
    b.push(1, b"a", arrival=0.0, expected=0.0)
    b.set_skew(0.1)
    assert b.drain(now=0.05) == []
    assert [e["seq"] for e in b.drain(now=0.2)] == [1]


def test_deadline_uses_buffer_depth():
    b = JitterBuffer()
    b.update_depth(200)  # depth 6 -> 120ms
    b.push(1, b"a", arrival=0.0, expected=0.0)
    assert b.drain(now=0.06) == []
