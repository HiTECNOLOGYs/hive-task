(in-package #:org.hitecnologys.hive-task)

(defparameter *scheduler-run-interval* 1/100) ; 10 ms sounds like about enough precison.
(defparameter *max-number-of-threads*  4)     ; Most common number of cores nowadays.
                                              ; TODO Make it actually guess number of cores CPU has.

(defvar       *scheduler-thread*       nil)
(defvar       *threads-pool*           (make-array *max-number-of-threads*))

(defvar       *work-queue*             (make-instance 'stmx.util:tfifo))
(defvar       *worker-messages*        (make-instance 'stmx.util:tchannel))

(define-constant +time-slice-duration+ 1/1000) ; That is 1 ms.
                                               ; About enough time for almost anything.
                                               ; All the time requirements are measured in slices.

;;; **************************************************************************
;;;  Classes
;;; **************************************************************************

(defclass scheduler-thread (thread) ())

(defclass worker-thread (thread)
  ((work-state :initform :waiting
               :type '(or :waiting  ; Waiting for new work.
                          :running  ; Running work.
                          :aborting ; Work ran out of time. Abort signal sent.
                          ))))

(defclass work ()
  ((function :initarg :function
             :initform (error "Work function must be supplied!"))
   (arguments :initarg :arguments
              :initform nil)
   (time-slices :initarg :time-slices
                :initform 1)))

(defclass message ()
  ((type :initarg :type
         :initform (error "Message must have type"))))

(defgeneric work-on (thread object))

;;; **************************************************************************
;;;  Messages
;;; **************************************************************************

(defmethod print-object ((object message) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "~A" (slot-value object 'type))))

(defun make-message (type)
  (make-instance 'message :type type))

(defmethod work-on ((thread worker-thread) (object message))
  (case (slot-value object 'type)
    (:stop (setf (slot-value thread 'state) :stopping))))

;;; **************************************************************************
;;;  Work
;;; **************************************************************************

;; TODO Make it intelligently guess how much take work is going to take.
;;      (Based on statistics from another calls of same functions, of course)

(defmethod print-object ((object work) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "~A (~{~A~^ ~})"
            (slot-value object 'function)
            (slot-value object 'arguments))))

(defun make-work (function &rest arguments)
  (make-instance 'work
                 :function function
                 :arguments arguments))

(defmethod work-on ((thread worker-thread) (object work))
  (with-slots (function arguments) object
    (if (null arguments)
      (funcall function)
      (apply function arguments))))

(defun handle-work-or-message (thread object)
  (with-slots (work-state) thread
    (setf work-state :running)
    (work-on thread object)
    (setf work-state :waiting)))

;;; **************************************************************************
;;;  Scheduler
;;; **************************************************************************

;; TODO Actually make it dynamically scale workers count.
;; TODO Implement time slice boundaries enforcement.

(defmacro with-time-window ((duration) &body body)
  "Ensures body runs exactly (or close to that) as long as supplied duration or
more (which is totally normal behaviour since scheduler doesn't need to run
periodically, it needs to run *at least* periodically to make sure workers are
running and respond to load change)."
  (with-gensyms (start-gensyms time-left-gensyms)
    `(let ((,start-gensyms (get-current-time)))
       ,@body
       (let ((,time-left-gensyms (- ,duration (delta-time (get-current-time) ,start-gensyms))))
         (when (< 0 ,time-left-gensyms)
           (sleep ,time-left-gensyms))))))

(defun scheduler-running-p ()
  (when (threadp *scheduler-thread*)
    (thread-running-p *scheduler-thread*)))

(defun make-scheduler ()
  (start-thread (make-thread 'scheduler-thread "Scheduler")))

(defun start-scheduler ()
  (unless (scheduler-running-p)
    (setf *scheduler-thread* (make-scheduler))))

(defun stop-scheduler ()
  (when (scheduler-running-p)
    (stop-thread *scheduler-thread*))
  (unless (scheduler-running-p)
    (setf *scheduler-thread* nil)))

(defmethod init-thread :after ((thread scheduler-thread))
  (start-workers))

(defmethod run-thread ((thread scheduler-thread))
  (do-while-running (thread)
    (with-time-window (*scheduler-run-interval*)
      (ensure-workers-are-running))))

(defmethod cleanup-thread :before ((thread scheduler-thread))
  (stop-workers))

;;; **************************************************************************
;;;  Workers
;;; **************************************************************************

(defun take-message ()
  (stmx.util:take *worker-messages*))

(defun put-message (message)
  (stmx.util:put *worker-messages* message))

(defun take-work ()
  (stmx.util:take *work-queue*))

(defun put-work (work)
  (stmx.util:put *work-queue* work))

(defun make-worker (id)
  (let ((thread (make-thread 'worker-thread (format nil "Worker ~A" id)))
        (port (make-instance 'stmx.util:tport :channel *worker-messages*)))
    (start-thread thread (list* (cons '*worker-messages* port)
                                bt:*default-special-bindings*))))

(defun ensure-workers-are-running ()
  (loop for i below *max-number-of-threads*
        for thread = (svref *threads-pool* i)
        unless (and (threadp thread) (thread-running-p thread))
          do (setf (svref *threads-pool* i) (make-worker i))))

(defun start-workers ()
  (ensure-workers-are-running))

(defun stop-workers ()
  (put-message (make-message :stop)))

(defmethod run-thread ((thread worker-thread))
  (do-while-running (thread)
    (handle-work-or-message thread
                            (atomic (run-orelse #'take-work #'take-message)))))
