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

(defmethod (setf tc:get-value) (new-value (key uuid:uuid) object)
  (setf (tc:get-value (uuid:uuid-to-byte-array key) object) new-value))

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
  ((uuid :initform (uuid:make-v4-uuid)
         :reader thread-uuid)
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
;;;  Threads pool
;;; **************************************************************************

(transactional
  (defclass threads-pool ()
    ((threads)
     (thread-count :initform 0)
     (pool-size))))

(define-condition pool-shrinking-error ()
  ((new-size :initarg :new-size)
   (pool :initarg :pool))
  (:report (lambda (condition stream)
             (with-slots (pool new-size) condition
               (format stream "More threads (~D) than new size: ~D."
                       (atomic (slot-value pool 'thread-count))
                       'new-size)))))

(define-condition pool-not-enough-space ()
  ((pool :initarg :pool)
   (thread :initarg :thread))
  (:report (lambda (condition stream)
             (with-slots (pool thread) condition
               (format stream "Pool ~S doesn't have enough space to hold ~A."
                       pool
                       thread)))))

(define-condition pool-n-exceeds-thread-count ()
  ((pool :initarg :pool)
   (n :initarg :n))
  (:report (lambda (condition stream)
             (with-slots (pool n) condition
               (format stream "Supplied thread number (~D) exceeds total number of threads in pool: ~D."
                       n
                       (atomic (slot-value pool 'thread-count)))))))

(defgeneric put-thread-into-pool (pool thread))
(defgeneric take-thread-from-pool (pool uuid))
(defgeneric take-random-thread-from-pool (pool))
(defgeneric remove-thread-from-pool (pool uuid))
(defgeneric resize-pool (pool new-size))
(defgeneric map-pool (pool function))

(defmethod initialize-instance :after ((instance threads-pool) &key initial-pool-size)
  (with-slots (threads thread-count pool-size) instance
    (atomic
     (setf pool-size initial-pool-size))
    (atomic
     (setf threads (make-list 0)))))

(defun push-thread (pool thread)
  (with-slots (threads thread-count) pool
    (atomic (push thread threads))
    (atomic (incf thread-count))))

(defun push-thread-extend (pool thread)
  (with-slots (pool-size) pool
    (resize-pool pool (1+ (atomic pool-size))))
  (push-thread pool thread))

(defmethod put-thread-into-pool ((pool threads-pool) thread)
  (with-slots (threads thread-count pool-size) pool
    (cond
      ((< thread-count pool-size)
       (push-thread pool thread))
      ((= thread-count pool-size)
       (restart-case
           (error 'pool-not-enough-space
                  :pool pool
                  :thread thread)
         (grow-pool ()
           :report "Grow pool."
           (push-thread-extend pool thread))
         (abort ()
           :report "Don't put."
           nil))))))

(defun find-thread-by-uuid (pool uuid)
  (with-slots (threads) pool
    (atomic
     (find uuid threads
           :test #'uuid:uuid=
           :key #'thread-uuid))))

(defmethod take-thread-from-pool (pool uuid)
  (with-slots (threads) pool
    (find-thread-by-uuid pool uuid)))

(defun find-nth-thread (pool n)
  (with-slots (threads thread thread-count) pool
    (when (>= n (atomic thread-count))
      (error 'pool-n-exceeds-thread-count
             :pool pool
             :n n))
    (atomic (elt threads n))))

(defmethod take-random-thread-from-pool (pool)
  (with-slots (threads thread-count) pool
    (atomic
     (find-nth-thread threads (random thread-count)))))

(defun remove-thread (pool uuid)
  (with-slots (threads) pool
    (atomic
     (loop for (thread . tail) on threads
           if (uuid:uuid= uuid (thread-uuid thread))
             do (setf (cdr buffer) tail)
                (return t)
           else
             collect thread into buffer))))

(defmethod remove-thread-from-pool (pool uuid)
  (with-slots (threads thread-count) pool
    (when (remove-thread pool uuid)
      (atomic (decf thread-count)))))

(defun read-new-size ()
  (format t "Enter new size: ")
  (multiple-value-list (eval (read))))

(defmethod resize-pool (pool new-size)
  (with-slots (thread-count pool-size) pool
    (if (< (atomic new-size) (atomic thread-count))
      (restart-case
          (error 'pool-shrinking-error
                 :pool pool
                 :new-size new-size)
        (change-new-size (changed-new-size)
          :report "Use another size."
          :interactive read-new-size
          (atomic
           (setf pool-size changed-new-size))))
      (atomic
       (setf pool-size new-size)))))

(defmethod map-pool (pool function)
  (with-slots (threads) pool
    (mapc function (atomic threads))))

(defmethod print-object ((object threads-pool) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (with-slots (pool-size thread-count) object
        (format stream "size: ~D holding: ~D"
                (atomic pool-size)
                (atomic thread-count)))))

;;; **************************************************************************
;;;  Scheduler
;;; **************************************************************************

(defclass scheduler-thread (thread)
  ((threads-pool :initform (make-instance 'threads-pool
                                          :initial-pool-size (get-cpu-core-count)))
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
  (unless (stmx:hw-transaction-supported?)
    (log:warn #.(concatenate 'string
                             "Seems like your machine doesn't have hardware STM support."
                             " Proceed only if you know what are you doing since severe performance penalties may arise."
                             " Consider using SIMPLE-TASKS instead: https://github.com/Shinmera/simple-tasks")))
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
      (put-thread-into-pool threads-pool worker)
      (with-slots (uuid) worker
        (atomic (setf (tc:get-value uuid message-ports) port))
        (atomic (setf (tc:get-value uuid message-channels) channel)))
      (start-thread worker))))

(defun stop-worker (port)
  (send-message port (make-system-event :stop)))

(defun start-workers (scheduler)
  (let ((cpu-count (get-cpu-core-count)))
    (loop repeat cpu-count doing
      (start-worker scheduler))))

(defun stop-workers (scheduler)
  (with-slots (message-ports) scheduler
    (tc:map-container #'stop-worker message-ports)))

(defun cleanup-after-worker (scheduler thread)
  (with-slots (uuid) thread
    (with-slots (threads-pool) scheduler
      (remove-thread-from-pool threads-pool uuid))
    (with-slots (message-ports message-channels) scheduler
      (atomic (tc:rem-value uuid message-ports))
      (atomic (tc:rem-value uuid message-channels)))))

(defun check-worker (scheduler thread)
  (unless (thread-running-p thread)
    (cleanup-after-worker scheduler thread)
    (start-worker scheduler)))

(defun check-workers (scheduler)
  (with-slots (threads-pool) scheduler
    (map-pool threads-pool (curry #'check-worker scheduler))))

(defmethod init-thread :after ((thread scheduler-thread))
  (start-workers thread))

(defmethod run-thread ((thread scheduler-thread))
  (do-while-running (thread)
    (with-slots (run-interval) thread
      (with-time-window (run-interval)
        (check-workers thread)))))

(defmethod cleanup-thread :before ((thread scheduler-thread))
  (stop-workers thread))
