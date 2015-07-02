(in-package #:org.hitecnologys.hive-task)

(defclass timer ()
  ((timer)
   (duration :initarg :duration
             :reader timer-duration)
   (absolute? :initarg :absolute?
              :reader timer-absolute-p))
  (:documentation "Stores information about timers so that timer can be started when needed."))

(defun make-timer (name function time &optional absolute?)
  #-sbcl
  (error "Sorry, too lazy to add support for other implementations.")
  (make-instance 'timer
                 :timer #+sbcl (sb-ext:make-timer function :name name) #-sbcl nil
                 :duration time
                 :absolute? absolute?))

(defun start-timer (timer)
  #-sbcl
  (error "Sorry, too lazy to add support for other implementations.")
  (with-slots (timer duration absolute?) timer
    #+sbcl
    (sb-ext:schedule-timer timer duration
                           :absolute-p absolute?)))

(defun stop-timer (timer)
  #-sbcl
  (error "Sorry, too lazy to add support for other implementations.")
  (with-slots (timer) timer
    #+sbcl
    (sb-ext:unschedule-timer timer)))
