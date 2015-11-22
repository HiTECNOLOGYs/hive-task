(in-package #:org.hitecnologys.hive-task)

(defun get-current-time ()
  (multiple-value-bind (sec msec) (sb-ext:get-time-of-day)
    (+ sec (/ msec 1000000))))

(defun delta-time (time-1 time-2)
  (abs (- time-1 time-2)))

(defmacro while (form &body body)
  `(do () ((not ,form))
     ,@body))

(defmacro with-time-window ((duration) &body body)
  "Ensures body runs exactly (or close to that) as long as supplied duration
or more (which is quite bad, actually, since it's scheduler who distributes
tasks and such behaviour usually means congestion)."
  (with-gensyms (start-gensyms time-left-gensyms)
    `(let ((,start-gensyms (get-current-time)))
       ,@body
       (let ((,time-left-gensyms (- ,duration (delta-time (get-current-time) ,start-gensyms))))
         (when (< 0 ,time-left-gensyms)
           (sleep ,time-left-gensyms))))))
