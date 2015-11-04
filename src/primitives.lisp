(in-package #:org.hitecnologys.transactional-containers)

;;; We only define bindings to STMX's implementations here in case we even
;;; need to switch to different implementation. We also only define bindigns
;;; to what we actually plan on using.

;;; **************************************************************************
;;;  Basic methods
;;; **************************************************************************

;; Generic container methods
(defgeneric full-p (object))
(defgeneric empty-p (object))
(defgeneric empty! (object))

;; Cons-specific methods
(defgeneric tcar (object))
(defgeneric tcdr (object))
(defgeneric (setf tcar) (new-value object))
(defgeneric (setf tcdr) (new-value object))

;; List-specific methods
(defgeneric tpush (value object))
(defgeneric tpop (object))

;; Ordered container methods
(defgeneric peek (object))
(defgeneric take (object))
(defgeneric put (value object))
(defgeneric try-take (object))
(defgeneric try-put (value object))

;; Key-value container methods
(defgeneric entry-count (object))
(defgeneric get-value (key object &optional default))
(defgeneric (setf get-value) (new-value key object))
(defgeneric set-value (new-value key object))
(defgeneric rem-value (key object))
(defgeneric map-container (function object))
(defgeneric copy-container (object))
(defgeneric container-keys (object))
(defgeneric container-values (object))
(defgeneric container-pairs (object))

;;; **************************************************************************
;;;  Basic classes
;;; **************************************************************************

(defclass transactional-container () ()
  (:documentation "Base transactional container class."))

(defclass transactional-ordered-container (transactional-container) ()
  (:documentation "Transactional container which data is stored or processed
in other way in some order."))

(defclass transactional-key-value-container (transactional-container) ()
  (:documentation "Transactional container which holds mappings between keys
and data, not necessarily in order."))

;;; **************************************************************************
;;;  Containers
;;; **************************************************************************

(defclass transactional-cell (transactional-ordered-container)
  (container)
  (:documentation "Simplest transactional container. Holds a single value of any type."))

(defclass transactional-cons (transactional-ordered-container)
  ((container :initform (stmx.util:tcons nil nil)))
  (:documentation "Transactional cons cell."))

(defclass transactional-list (transactional-ordered-container)
  ((container :initform (stmx.util:tlist)))
  (:documentation "Transactional list."))

(defclass transactional-filo-queue (transactional-ordered-container)
  ((container :initform (make-instance 'stmx.util:tstack)))
  (:documentation "FIFO queue."))

(defclass transactional-fifo-queue (transactional-ordered-container)
  ((container :initform (make-instance 'stmx.util:tfifo)))
  (:documentation "FIFO queue."))

(defclass transactional-channel (transactional-ordered-container)
  ((container :initform (make-instance 'stmx.util:tchannel)))
  (:documentation "Transactional multicast channel."))

(defclass transactional-port (transactional-ordered-container)
  (container)
  (:documentation "Transactional multicast port."))

(defclass transactional-hash-table (transactional-key-value-container)
  (container)
  (:documentation "Transactional hash table."))

(defclass transactional-map (transactional-key-value-container)
  (container
   (predicate :initarg :predicate
              :initform #'stmx.util:fixnum<
              :reader transactional-map-predicate))
  (:documentation "Transactional sorted map."))

;;; **************************************************************************
;;;  Container initialization methods
;;; **************************************************************************

(defmethod initialize-instance :after ((instance transactional-cell) &key initial-value)
  (with-slots (container) instance
    (setf container (if initial-value
                      (make-instance 'stmx.util:tcell
                                     :value initial-value)
                      (make-instance 'stmx.util:tcell)))))

(defmethod initialize-instance :after ((instance transactional-cons) &key initial-car initial-cdr)
  (with-slots (container) instance
    (setf container (stmx.util:tcons initial-car initial-cdr))))

(defmethod initialize-instance :after ((instance transactional-list) &key initial-data)
  (with-slots (container) instance
    (setf container (apply #'stmx.util:tlist initial-data))))

(defmethod initialize-instance :after ((instance transactional-port) &key channel)
  (unless channel
    (error "Port cannot exist without linked channel"))
  (with-slots (container) channel
    (setf container (make-instance 'stmx.util:tport
                                   :channel (slot-value channel 'container)))))

(defmethod initialize-instance :after ((instance transactional-hash-table) &key test hash)
  (unless test
    (error "Test function must be supplied for hash table to work"))
  (with-slots (container) instance
    (setf container (make-instance 'stmx.util:thash-table
                                   :test test
                                   :hash hash))))

(defmethod initialize-instance :after ((instance transactional-map) &key predicate)
  (unless predicate
    (error "Preducate must be supplied for map to work"))
  (with-slots (container) instance
    (setf container (make-instance 'stmx.util:tmap
                                   :pred predicate))))

;;; **************************************************************************
;;;  Generic methods
;;; **************************************************************************

(defmethod full-p ((object transactional-container))
  (with-slots (container) object
    (stmx.util:full? container)))

(defmethod empty-p ((object transactional-container))
  (with-slots (container) object
    (stmx.util:empty? container)))

(defmethod empty! ((object transactional-container))
  (with-slots (container) object
    (stmx.util:empty! container)))

;;; **************************************************************************
;;;  Ordered container methods
;;; **************************************************************************

(defmethod peek ((object transactional-ordered-container))
  (with-slots (container) object
    (stmx.util:peek container)))

(defmethod take ((object transactional-ordered-container))
  (with-slots (container) object
    (stmx.util:take container)))

(defmethod put (value (object transactional-ordered-container))
  (with-slots (container) object
    (stmx.util:put container value)))

(defmethod try-take ((object transactional-ordered-container))
  (with-slots (container) object
    (stmx.util:try-take container)))

(defmethod try-put (value (object transactional-ordered-container))
  (with-slots (container) object
    (stmx.util:try-put container value)))

;;; **************************************************************************
;;;  Cons-specific methods
;;; **************************************************************************

(defmethod tcar ((object transactional-cons))
  (with-slots (container) object
    (stmx.util:tfirst container)))

(defmethod tcdr ((object transactional-cons))
  (with-slots (container) object
    (stmx.util:trest container)))

(defmethod (setf tcar) (new-value (object transactional-cons))
  (with-slots (container) object
    (setf (stmx.util:tfirst container) new-value)))

(defmethod (setf tcdr) (new-value (object transactional-cons))
  (with-slots (container) object
    (setf (stmx.util:trest container) new-value)))

;;; **************************************************************************
;;; List-specific methods
;;; **************************************************************************

(defmethod tpush (value (object transactional-list))
  (with-slots (container) object
    (stmx.util:tpush value container)))

(defmethod tpop ((object transactional-list))
  (with-slots (container) object
    (stmx.util:tpop container)))

;;; **************************************************************************
;;;  Key-value container methods
;;; **************************************************************************

(defmethod entry-count ((object transactional-hash-table))
  (with-slots (container) object
    (stmx.util:ghash-table-count container)))

(defmethod entry-count ((object transactional-map))
  (with-slots (container) object
    (stmx.util:gmap-count container)))

(defmethod empty-p ((object transactional-hash-table))
  (with-slots (container) object
    (stmx.util:ghash-table-empty? container)))

(defmethod empty-p ((object transactional-map))
  (with-slots (container) object
    (stmx.util:gmap-empty? container)))

(defmethod empty! ((object transactional-hash-table))
  (with-slots (container) object
    (stmx.util:clear-ghash container)))

(defmethod empty! ((object transactional-map))
  (with-slots (container) object
    (stmx.util:clear-gmap container)))

(defmethod get-value (key (object transactional-hash-table) &optional default)
  (with-slots (container) object
    (stmx.util:get-ghash container key default)))

(defmethod get-value (key (object transactional-map) &optional default)
  (with-slots (container) object
    (stmx.util:get-gmap container key default)))

(defmethod (setf get-value) (new-value key (object transactional-hash-table))
  (with-slots (container) object
    (setf (stmx.util:get-ghash container key) new-value)))

(defmethod (setf get-value) (new-value key (object transactional-map))
  (with-slots (container) object
    (setf (stmx.util:get-gmap container key) new-value)))

(defmethod set-value (new-value key (object transactional-hash-table))
  (with-slots (container) object
    (stmx.util:set-ghash container key new-value)))

(defmethod set-value (new-value key (object transactional-map))
  (with-slots (container) object
    (stmx.util:set-gmap container key new-value)))

(defmethod rem-value (key (object transactional-hash-table))
  (with-slots (container) object
    (stmx.util:rem-ghash container key)))

(defmethod rem-value (key (object transactional-map))
  (with-slots (container) object
    (stmx.util:rem-gmap container key)))

(defmethod map-container (function (object transactional-hash-table))
  ;; Surprisingly, this isn't present in STMX.UTIL package. =P
  ;; We need to define MAP-GHASH outselves.
  (with-slots (container) object
    (loop for key in (stmx.util:gmap-keys object)
          collecting (funcall function (stmx.util:get-gmap container key)))))

(defmethod map-container (function (object transactional-map))
  (with-slots (container) object
    (stmx.util:map-gmap container function)))

(defmethod copy-container ((object transactional-hash-table))
  ;; Surprisingly, this isn't present in STMX.UTIL package as well.
  ;; We need to define COPY-GHASH outselves too.
  (with-slots (container) object
    (let ((new-ghash (make-instance 'stmx.util:thash-table)))
      (stmx.util:do-ghash (key value) container
        (setf (stmx.util:get-ghash new-ghash key) value))
      new-ghash)))

(defmethod copy-container ((object transactional-map))
  (with-slots (container) object
    (stmx.util:copy-gmap container)))

(defmethod container-keys ((object transactional-hash-table))
  (with-slots (container) object
    (stmx.util:ghash-keys container)))

(defmethod container-keys ((object transactional-map))
  (with-slots (container) object
    (stmx.util:gmap-keys container)))

(defmethod container-values ((object transactional-hash-table))
  (with-slots (container) object
    (stmx.util:ghash-values container)))

(defmethod container-values ((object transactional-map))
  (with-slots (container) object
    (stmx.util:gmap-values container)))

(defmethod container-pairs ((object transactional-hash-table))
  (with-slots (container) object
    (stmx.util:ghash-pairs container)))

(defmethod container-pairs ((object transactional-map))
  (with-slots (container) object
    (stmx.util:gmap-pairs container)))

;;; **************************************************************************
;;;  Functional wrappers
;;; **************************************************************************

(defun tcons (car cdr)
  (make-instance 'transactional-cons
                 :initial-car car
                 :initial-cdr cdr))

(defun tlist (&rest data)
  (make-instance 'transactional-list
                 :initial-data data))
