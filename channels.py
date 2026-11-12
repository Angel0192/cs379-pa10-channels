"""
PA 10: Channels -- starter (final PA of the semester).

Complete Channel below, plus the producer/consumer driver at the
bottom. See the assignment, Part B, for the full requirements.
Values passed through the channel are GCHeap handles (ints) -- see
gc_heap.py.
"""

import threading
from collections import deque
from typing import Any, Optional

from gc_heap import GCHeap

_CLOSED_SENTINEL = object()


class Channel:
    """
    A bounded, thread-safe FIFO channel of GCHeap handles.

    Reference rule: while a handle sits in the buffer, the channel owns
    exactly one reference to it. put() increfs on the way in; get() hands
    that same reference to the caller with no count change.
    """

    def __init__(self, capacity: int, heap: GCHeap) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self.capacity = capacity
        self.heap = heap
        self._buffer: deque = deque()
        self._closed = False
        # One lock shared by two "waiting rooms": producers wait on
        # _not_full, consumers wait on _not_empty.
        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)

    def put(self, value: Any) -> None:
        """
        Block while the buffer is full. `value` is expected to be a
        GCHeap handle - incref it (the channel now holds a reference)
        BEFORE enqueuing. Wake a waiting get() once space is available.
        """
        with self._lock:
            # A while loop (not if): after waking, re-check the condition,
            # since another producer may have taken the free slot first.
            while len(self._buffer) >= self.capacity and not self._closed:
                self._not_full.wait()  # sleeps and releases the lock
            if self._closed:
                # Nothing was increfed, so the caller still owns its reference.
                raise RuntimeError("put on a closed channel")
            # Open and there's room: the channel takes its own reference.
            self.heap.incref(value)
            self._buffer.append(value)
            self._not_empty.notify()  # wake exactly one waiting consumer

    def get(self):
        """
        Block while the buffer is empty (and not closed). Returns the
        next value, transferring the channel's reference to the
        caller (do NOT decref here - the caller now owns that
        reference and is responsible for eventually releasing it).
        On an empty, closed channel, return the sentinel from
        `Channel.CLOSED` instead of blocking forever.
        """
        with self._lock:
            while not self._buffer and not self._closed:
                self._not_empty.wait()
            if not self._buffer:
                # Empty and closed: nothing left will ever arrive.
                return Channel.CLOSED
            # Buffered items drain first, even after close().
            value = self._buffer.popleft()
            self._not_full.notify()  # a slot opened: wake one producer
            return value

    def close(self) -> None:
        """Mark closed and wake every blocked thread."""
        with self._lock:
            self._closed = True
            # Everyone wakes up, re-checks, and sees the closed flag.
            self._not_full.notify_all()
            self._not_empty.notify_all()

    CLOSED = _CLOSED_SENTINEL


def producer_consumer_driver(heap: GCHeap, values: list, channel: Channel, num_consumers: int = 1):
    """
    Given: a driver you can use as-is (or adapt) for your own testing.
    One producer thread allocates + puts each value in `values` onto
    `channel`, then closes it. `num_consumers` consumer threads pull
    handles off, look up and record their value, then release their
    transferred reference, exiting cleanly on the CLOSED sentinel.
    Returns the list of delivered (handle, value) pairs.
    """
    delivered = []
    delivered_lock = threading.Lock()

    def produce():
        for v in values:
            handle = heap.allocate(v)
            try:
                channel.put(handle)
            finally:
                heap.decref(handle)  # release the creator reference
        channel.close()

    def consume():
        while True:
            handle = channel.get()
            if handle is Channel.CLOSED:
                return
            value = heap._objects[handle][0]  # PA 9 stores [object, refcount]
            with delivered_lock:
                delivered.append((handle, value))
            heap.decref(handle)  # the consumer is done with its transferred reference

    producer_thread = threading.Thread(target=produce)
    consumer_threads = [threading.Thread(target=consume) for _ in range(num_consumers)]
    for t in [producer_thread] + consumer_threads:
        t.start()
    producer_thread.join()
    for t in consumer_threads:
        t.join(timeout=10.0)

    return delivered