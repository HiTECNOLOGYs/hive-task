(in-package #:org.hitecnologys.hive-task)

(defclass thread ()
  ((name :initarg :name
         :reader thread-name)
   (state :initform :stopped
          :reader thread-state
          :type (member :stopped   ; Terminated by scheduler or not started yet
                        :starting  ; Thread initializing
                        :running   ; Running
                        :stopping  ; Signal sent, waiting thread to terminate itself
                        ))
   (object)))

(defmethod print-object ((object thread) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "~A (~A)"
            (slot-value object 'name)
            (slot-value object 'state))))

(defmacro with-thread-signals ((thread) &body body)
  `(handler-bind
     ((thread-stop
        #'(lambda (condition)
            (declare (ignore condition))
            (change-thread-state ,thread :stopping))))
     ,@body))

(defun threadp (object)
  (when (typep object 'thread)
    t))

(defun change-thread-state (thread new-state)
  (setf (slot-value thread 'state) new-state))

(defun thread-state= (thread state)
  (eql (slot-value thread 'state) state))

(defgeneric init-thread (thread)
  (:documentation "Called to initialize thread context.")
  (:method ((thread thread))
   (change-thread-state thread :starting)
   ;; Do init here
   ))

(defgeneric run-thread (thread)
  (:documentation "Called when thread forks to do its work.")
  (:method :around ((thread thread))
   (unwind-protect
     (with-thread-signals (thread)
       (init-thread thread)
       (change-thread-state thread :running)
       (call-next-method))
     (cleanup-thread thread))))

(defgeneric cleanup-thread (thread)
  (:documentation "Called when thread dies.")
  (:method ((thread thread))
   (change-thread-state thread :stopped)))

(defgeneric thread-running-p (thread)
  (:method ((thread thread))
   (thread-state= thread :running)))

(define-condition thread-stop () ()
  (:documentation "Signaled to tell the thread that it should terminate itself."))

(defun make-thread (class &optional name)
  (make-instance class :name name))

(defun start-thread (thread &optional (initial-bindings bt:*default-special-bindings*))
  (unless (thread-running-p thread)
    (with-slots (object name) thread
      (setf object (bt:make-thread (curry #'run-thread thread)
                                   :name name
                                   :initial-bindings initial-bindings))))
  thread)

(defun stop-thread/inside (thread)
  "Used to stop thread from inside since the normal version would deadlock."
  (change-thread-state thread :stopping)
  thread)

(defun stop-thread (thread &optional (block? t))
  (when (thread-running-p thread)
    (with-slots (object) thread
      ;; Probably the only safe way to stop thread is signal it and wait until
      ;; it stops itself.
      (bt:interrupt-thread object #'(lambda () (signal 'thread-stop)))
      (when block?
        (bt:join-thread object))))
  thread)

(defmacro do-while-running ((thread) &body body)
  `(while (thread-running-p ,thread)
     ,@body))
