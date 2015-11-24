(in-package #:org.hitecnologys.hive-task)

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
