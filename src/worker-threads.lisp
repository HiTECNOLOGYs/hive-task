(in-package #:org.hitecnologys.hive-task)

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
  (handler-case
      (unwind-protect
         (progn (change-thread-work-state thread :running)
                (call-next-method))
      (change-thread-work-state thread :waiting))
    (error (condition)
      (record-crash thread condition))))

(defmethod handle-message ((thread worker-thread) (message work-data))
  (log:trace "Message received by ~A: ~A" thread message)
  (with-slots (function arguments) message
    (if (null arguments)
      (funcall function)
      (apply function arguments))))
