hive-task
=========

Hive-task is a set of primitives for building multitasking applications using
STM. Mostly it's planned to be similar to lparallel in many aspects but with
STM instead of locks.

Note to reader: it sounds like a good plan for someone to write similar system
using lock-free algorithms for the sake of completeness and further comparison.

Usage
-----

To start or stop task scheduler, use

```lisp
;; Start
(hive-task:start-scheduler)

;; Stop
(hive-task:stop-scheduler)
```

Scheduler will automatically start worker threads and monitor their activity.

To put some actual work, use

```lisp
;; Example 1
(put-work (make-work #'foo))

;; Example 2
(put-work (make-work #'bar "arg-1" 'arg-2 3)
```

Status
------

Currently, this is pretty much it.

All in all, I'm planning to implement the following features (not in order):

* [✓] Worker threads
* [✗] Better scheduler (dynamic load management, worker thread profiling, work monitoring)
* [✗] Events (workers are not supposed to put work, instead they should schedule events)
* [✗] Cooperative multitasking (not exactly what is usually meant by this term: many threads may work on one job and exchange data if necessary)
* [✗] Automatic scaling (using hardware detection to get the most out of machine it's running on)
* [✗] Support for non-general-purpose processors for computation acceleration (GPUs, FGPAs, etc)
* [✗] More multitasking primitives (like various work types that allow tweaking performance and memory consumption better)
* Haven't though of anything else to add yet

It's important to notice that I'm not focused on delivering solution for
migration of single-threaded application to multi-threaded environments (yet).
Instead, this library is meant to help developers who are in desperate need of
paralleling tasks that are meant to be paralleled like heavy computations or
handling network messages. In fact, The original reason I wrote this was to
manage threads in my networking library I use for several servers of mine.
