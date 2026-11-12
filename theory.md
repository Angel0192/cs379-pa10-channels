# PA 10: Channels — Part A

**CS 379: Programming Languages, Fall 2026**
**Angel Lopez**

## 1. Bounded Buffer Trace

Capacity is 2.

| Step | Buffer after | Notes |
|---|---|---|
| `P.put(1)` | `[1]` | room available |
| `P.put(2)` | `[1, 2]` | buffer is now full |
| `P.put(3)` starts | `[1, 2]` | **P blocks here** |
| `C.get()` → 1 | `[2]` | frees a slot and wakes P |
| `P.put(3)` completes | `[2, 3]` | **P proceeds here** |
| `C.get()` → 2 | `[3]` | FIFO, so 2 comes out next |

P blocks as soon as `put(3)` sees the buffer holding 2 items, before 3 is added. It's allowed to continue right after `C.get()` removes 1 and signals that a slot is free.

## 2. Deadlock by Design

The inner `get()` runs first, so both threads start by trying to read from an empty channel.

```
Thread 1: blocked in ch_a.get()  ──waits for──▶  someone to put into ch_a
                                                   (only Thread 2 does that)
Thread 2: blocked in ch_b.get()  ──waits for──▶  someone to put into ch_b
                                                   (only Thread 1 does that)
```

Thread 1 can't put into `ch_b` until it gets from `ch_a`, and Thread 2 can't put into `ch_a` until it gets from `ch_b`. Each is waiting on the other, so neither ever moves.

**Fix:** make one thread produce first. For example, Thread 1 does `ch_b.put(start_value)` before reading `ch_a`. Now Thread 2's `ch_b.get()` succeeds, it puts into `ch_a`, and Thread 1 can continue. The cycle breaks because one of the two waits no longer depends on the other thread.

## 3. Bounded vs. Unbounded, and the GC Connection

In an unbounded channel, every `put` increfs the handle and the channel holds that reference until someone calls `get`. If the producer is faster than the consumer, the buffer keeps growing, and every object in it stays at refcount ≥ 1 because the channel is holding it.

Nothing is technically leaked, since every object is still reachable and will be freed once it's read. But memory still keeps growing without a limit. A bounded channel fixes this by making the producer wait, which caps how many references the buffer can hold at once.