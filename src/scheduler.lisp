(in-package #:org.hitecnologys.hive-task)

(defvar *scheduler* nil
  "Optionally, scheduler can be bound to this variable and then you won't have
to pass it to every single function call.")

(define-constant +time-slice-duration+ 1/1000) ; That is 1 ms.
                                               ; About enough time for almost anything.
                                               ; All the time requirements are measured in slices.

;;; **************************************************************************
;;;  UUID support for transactional containers
;;; **************************************************************************

(defmethod tc:get-value ((key uuid:uuid) object &optional default)
  (tc:get-value (uuid:uuid-to-byte-array key) object default))

(defmethod tc:set-value (new-value (key uuid:uuid) object)
  (tc:set-value new-value (uuid:uuid-to-byte-array key) object))

(defmethod tc:rem-value ((key uuid:uuid) object)
  (tc:rem-value (uuid:uuid-to-byte-array key) object))

;;; **************************************************************************
;;;  Hardware detection
;;; **************************************************************************

(defun get-cpu-core-count ()
  #+linux (let* ((cpu-info (com.informatimago.clmisc.resource-utilization::cpu-info))
                 (core-count-str (cdr (assoc :cpu-cores cpu-info)))
                 (core-count-int (parse-integer core-count-str)))
            core-count-int)
  #-linux 4 ; Defaults to 4 for non-linux systems since it's a popular number of cores.
  )

;;; **************************************************************************
;;;  Messages
;;; **************************************************************************

(defclass work-data (message)
  ((function :initarg :function
             :initform (error "Work function must be supplied!"))
   (arguments :initarg :arguments
              :initform nil)
   (time-slices :initarg :time-slices
                :initform 10)))

(defclass system-event (message)
  ((type :initarg :type
         :type (member :stop    ; scheduler -> workers
                       :crashed ; workers -> scheduler
                       )
         :initform (error "Message must have type"))))

(defmethod print-object ((object work-data) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "~A(~{~A~^ ~})"
            (slot-value object 'function)
            (slot-value object 'arguments))))

(defmethod print-object ((object system-event) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "~A" (slot-value object 'type))))

(defgeneric handle-message (thread message))

(define-condition undefined-system-event ()
  ((event :initarg :event))
  (:report (lambda (condition stream)
             (format stream "Undefined system event came though: ~S"
                     (slot-value condition 'type)))))

(defun make-system-event (type)
  (make-instance 'system-event
                 :type type))

(defun make-work-data (function &rest arguments)
  (make-instance 'work-data
                 :function function
                 :arguments arguments))

;;; **************************************************************************
;;;  Workers
;;; **************************************************************************

(defclass worker-thread (thread)
  ((uuid :initform (uuid:make-v4-uuid))
   (message-port :initarg :message-port
                 :reader worker-thread-message-port)
   (port-class :initarg :port-class
               :initform 'message-port/local)
   (scheduler :initarg :scheduler)
   (failure-condition :initform nil)
   (work-state :initform :waiting
               :type (member :waiting  ; Waiting for new work.
                             :running  ; Running work.
                             :crashed  ; Crashed
                             :aborting ; Work ran out of time. Abort signal sent.
                             ))))

(defmethod initialize-instance :after ((instance worker-thread) &key name-format)
  (when (and (not (slot-boundp instance 'name)) name-format)
    (with-slots (name uuid) instance
      (setf name (format nil name-format uuid))))
  (unless (slot-boundp instance 'message-port)
    (with-slots (message-port port-class) instance
      (setf message-port (make-instance port-class)))))

(defun make-worker (&key port-class)
  (if (null port-class)
    (make-instance 'worker-thread
                   :name-format "Worker [~A]")
    (make-instance 'worker-thread
                   :port-class port-class
                   :name-format "Worker [~A]")))

(defun change-thread-work-state (thread new-state)
  (with-slots (work-state) thread
    (setf work-state new-state)))

(defmethod run-thread ((thread worker-thread))
  (do-while-running (thread)
    (let* ((port (worker-thread-message-port thread))
           (message (receive-message port)))
      (handle-message thread message))))

(defmethod handle-message ((thread worker-thread) (message system-event))
  (log:trace "Message received by ~A: ~A" thread message)
  (case (slot-value message 'type)
    (:stop (change-thread-state thread :stopping))
    (otherwise (error 'undefined-system-event :event message))))

(defun record-crash (thread condition)
  (log:error "Thread ~A crashed: ~A" thread condition)
  (with-slots (failure-condition) thread
    (setf failure-condition condition))
  (send-message (worker-thread-message-port thread)
                (make-system-event :crashed)))

;; Setting up safe context around the work.
(defmethod handle-message :around ((thread worker-thread) (message work-data))
  (log:trace "Message received by ~A: ~A" thread message)
  (handler-bind
      ((error (curry #'record-crash thread)))
    (unwind-protect
         (progn (change-thread-work-state thread :running)
                (call-next-method))
      (change-thread-work-state thread :waiting))))

(defmethod handle-message ((thread worker-thread) (message work-data))
  (log:trace "Message received by ~A: ~A" thread message)
  (with-slots (function arguments) message
    (if (null arguments)
      (funcall function)
      (apply function arguments))))

;;; **************************************************************************
;;;  Scheduler
;;; **************************************************************************

(defclass scheduler-thread (thread)
  ((threads-pool :initform (make-instance 'tc:transactional-hash-table
                                          :test 'equalp))
   (message-channels :initform (make-instance 'tc:transactional-hash-table
                                              :test 'equalp))
   (message-ports :initform (make-instance 'tc:transactional-hash-table
                                           :test 'equalp))
   (channel-class :initarg :channel-class
                  :initform 'message-channel/local)
   (port-class :initarg :port-class
               :initform 'message-port/local)
   (run-interval :initarg :run-interval
                 :initform 1/1000 ; 1 ms worth of precision.
                 )))

;; TODO Make it intelligently guess how much time work is going to take.
;;      (Based on statistics from another calls of same functions, of course)
;; TODO Actually make it dynamically scale workers count.
;; TODO Implement time slice boundaries enforcement.

(defun make-scheduler ()
  (make-thread 'scheduler-thread "Scheduler"))

(defun start-scheduler (scheduler)
  (unless (thread-running-p scheduler)
    (start-thread scheduler)))

(defun stop-scheduler (scheduler)
  (when (thread-running-p scheduler)
    (stop-thread scheduler)))

(defun start-worker (scheduler)
  (with-slots (threads-pool message-ports message-channels channel-class port-class) scheduler
    (let ((worker (make-worker :port-class port-class))
          (channel (make-instance channel-class))
          (port (make-instance port-class)))
      (connect-port (worker-thread-message-port worker) channel)
      (connect-port port channel)
      (with-slots (uuid) worker
        (atomic
         (after-commit
           (start-thread worker))
         (atomic (setf (tc:get-value uuid message-ports) port))
         (atomic (setf (tc:get-value uuid threads-pool) worker))
         (atomic (setf (tc:get-value uuid message-channels) channel)))))))

(defun stop-worker (port)
  (send-message port (make-system-event :stop)))

(defun start-workers (scheduler)
  (let ((cpu-count (get-cpu-core-count)))
    (loop repeat cpu-count doing
      (start-worker scheduler))))

(defun stop-workers (scheduler)
  (with-slots (message-ports) scheduler
    (tc:map-container #'stop-worker message-ports)))

(defun check-worker (scheduler thread)
  (unless (thread-running-p thread)
    (with-slots (threads-pool) scheduler
      (with-slots (uuid) thread
        (tc:rem-value uuid threads-pool)))
    (start-worker scheduler)))

(defun check-workers (scheduler)
  (with-slots (threads-pool) scheduler
    (tc:map-container (curry #'check-worker scheduler) threads-pool)))

(defmethod init-thread :after ((thread scheduler-thread))
  (start-workers thread))

(defmethod run-thread ((thread scheduler-thread))
  (do-while-running (thread)
    (with-slots (run-interval) thread
      (with-time-window (run-interval)
        (check-workers thread)))))

(defmethod cleanup-thread :before ((thread scheduler-thread))
  (stop-workers thread))
