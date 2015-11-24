(in-package #:org.hitecnologys.hive-task)

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
     (find-nth-thread pool (random thread-count)))))

(defun remove-thread (pool uuid)
  (with-slots (threads) pool
    (atomic
     (loop for (thread . tail) on threads
           if (uuid:uuid= uuid (thread-uuid thread))
             do (if (not (consp buffer))
                  (setf threads tail)
                  (setf (cdr buffer) tail
                        threads buffer))
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
