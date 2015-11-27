(in-package #:org.hitecnologys.hive-task)

(defvar *scheduler* nil
  "Used to hold current *SCHEDULER* instance.
Hive-task itself doesn't handle this and it's up to user whether to store
anything there or not.")

(define-constant +time-slice-duration+ 1/1000) ; That is 1 ms.
                                               ; About enough time for almost anything.
                                               ; All the time requirements are measured in slices.

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
   (work-queue :initarg :work-queue
               :initform (make-instance 'tc:transactional-fifo-queue))
   (run-interval :initarg :run-interval
                 :initform 1/1000 ; 1 ms worth of precision.
                 )))

(defgeneric put-work (scheduler work))

;; TODO Make it intelligently guess how much time work is going to take.
;;      (Based on statistics from another calls of same functions, of course)
;; TODO Actually make it dynamically scale workers count.
;; TODO Implement time slice boundaries enforcement.

(defun make-scheduler (&key
                       (run-interval 1/1000)
                       (channel-class 'message-channel/local)
                       (port-class 'message-port/local))
  "Creates and initializes work scheduler. The scheduler is a thread that
starts all the worker threads and monitors their state (i.e. restarting
crashed threads) as well as passes work to threads. This approach was chosen
to allow remote work execution (since very little computes compared to total
number of computers out there are RDMA capable). The strategy used by
scheduler is chosing random worker and then giving work to it.

You can then start this scheduler using START-SCHEDULER. It might also be wise
to store the instance somewhere. Hive-task provides *SCHEDULER* variable for
that.

Can also configure some important schedulering parameters:

* RUN-INTERVAL: defines how frequently scheduler is run. Default option is 1
  ms which makes sense for most of the cases but if you're handling heavy load
  it's recommended to increase it to, say, 10 ms or more. Basically, the rule of
  thumb is: keep it 10 times more than average work takes. There's no
  explanation for that rule, I just made it up. If you need more precise value,
  you should profile the scheduler and measure how much overhead it adds and
  adjust value to keep it below your acceptable value.

* CHANNEL-CLASS: defines class used to create instances of channel that is
  used for message transport. Since this has little usage now, it's
  recommended not to touch it, however it will be used later to specify medium
  used to carry messages.

* PORT-CLASS: defines class used to create instances of ports that are used as
  an interface to channel. These have little usage as well so you should only
  change this if you need to replace my implementation with more efficient
  one."
  (unless (stmx:hw-transaction-supported?)
    (log:warn #.(concatenate 'string
                             "Seems like your machine doesn't have hardware STM support."
                             " Proceed only if you know what are you doing since severe performance penalties may arise."
                             " Consider using SIMPLE-TASKS instead: https://github.com/Shinmera/simple-tasks")))
  (make-instance 'scheduler-thread
                 :name "Scheduler"
                 :run-interval run-interval
                 :channel-class channel-class
                 :port-class port-class))

(defun start-scheduler (scheduler)
  "Starts the work scheduler.
It also checks whether scheduler is already running. If it is, it doesn't do anything.

See MAKE-SCHEDULER for more details on scheduler."
  (unless (thread-running-p scheduler)
    (start-thread scheduler)))

(defun stop-scheduler (scheduler)
  "Stops work scheduler.
It also checks whether scheduler is already running. If it isn't, it doesn't do anything.

See MAKE-SCHEDULER for more details on scheduler."
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

(defun cleanup-after-worker (scheduler thread)
  (with-slots (uuid) thread
    (with-slots (threads-pool) scheduler
      (remove-thread-from-pool threads-pool uuid))
    (with-slots (message-ports message-channels) scheduler
      (atomic (tc:rem-value uuid message-ports))
      (atomic (tc:rem-value uuid message-channels)))))

(defun start-workers (scheduler)
  (let ((cpu-count (get-cpu-core-count)))
    (loop repeat cpu-count doing
      (start-worker scheduler))))

(defun stop-workers (scheduler)
  (with-slots (threads-pool message-ports) scheduler
    (atomic
     (tc:map-container #'stop-worker message-ports))
    (atomic
     (tc:map-container (curry #'cleanup-after-worker scheduler) threads-pool))))

(defun check-worker (scheduler thread)
  (unless (thread-running-p thread)
    (cleanup-after-worker scheduler thread)
    (start-worker scheduler)))

(defun check-workers (scheduler)
  (with-slots (threads-pool) scheduler
    (map-pool threads-pool (curry #'check-worker scheduler))))

(defun put-work-to-queue (scheduler work)
  (with-slots (work-queue) scheduler
    (atomic (tc:put work work-queue))))

(defun take-work-from-queue (scheduler &optional (block? nil))
  (with-slots (work-queue) scheduler
    (if block?
      (atomic (tc:take work-queue))
      (atomic (tc:try-take work-queue)))))

(defun distribute-work (scheduler work)
  (with-slots (threads-pool message-ports) scheduler
    (let* ((thread (take-random-thread-from-pool threads-pool))
           (uuid (atomic (thread-uuid thread)))
           (port (atomic (tc:get-value uuid message-ports))))
      (send-message port work))))

(defun check-work (scheduler)
  (with-slots (work-queue) scheduler
    (multiple-value-bind (available? work)
        (take-work-from-queue scheduler)
      (when available?
        (distribute-work scheduler work)))))

(defmethod init-thread :after ((thread scheduler-thread))
  (start-workers thread))

(defmethod run-thread ((thread scheduler-thread))
  (do-while-running (thread)
    (with-slots (run-interval) thread
      (with-time-window (run-interval)
        (check-workers thread)
        (check-work thread)))))

(defmethod cleanup-thread :before ((thread scheduler-thread))
  (stop-workers thread))

(defmethod put-work ((scheduler scheduler-thread) (work work-data))
  "Sends work to scheduler for futher distrubition to workers.

See MAKE-WORK for more details on work."
  (put-work-to-queue scheduler work))

(defun make-work (function &rest arguments)
  "Creates work out of function and arguments.
The work can the be executes using PUT-WORK.

See MAKE-SCHEDULER for more details on work execution."
  (apply #'make-work-data function arguments))
