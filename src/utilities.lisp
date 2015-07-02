(in-package #:org.hitecnologys.hive-task)

(defun get-current-time ()
  (multiple-value-bind (sec msec) (sb-ext:get-time-of-day)
    (+ sec (/ msec 1000000))))

(defun delta-time (time-1 time-2)
  (abs (- time-1 time-2)))

(defmacro while (form &body body)
  `(do () ((not ,form))
     ,@body))
