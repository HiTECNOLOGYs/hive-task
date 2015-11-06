(in-package #:org.hitecnologys.hive-task)

(defclass message-channel ()
  ((uuid :initform (uuid:make-v4-uuid)))
  (:documentation "Provides medium of transport between ports. It can either
be simple in-memory queue or AMQP broker, it's up to user to implement
required transport on top. The default implementation assumes that one channel
can only have two ports attached at the same time but this isn't enforced so
some users may choose to implement ability to attach more than two ports to a
channel making it many-to-many channel."))

(defclass message-port ()
  ((uuid :initform (uuid:make-v4-uuid))
   (channel :initarg channel
            :initform nil))
  (:documentation "Provides a way to connect to channel to send or receive
data. The implementation is up to user as well. The default implementation
assumes that port can only be connected to one channel at a time but this
isn't enforced so some users may choose to implement ability to attach it to
multiple channels at the same time."))

(defclass message () ()
  (:documentation "Generic message. This is what is sent over channels
via ports. All the messages must be heirs of at least this class."))

(defgeneric connect-port (port channel)
  (:documentation "Connects port to given channel. Initialized transport.
After this call the port should be ready to send and receive data. Such a
connection CANNOT change channel status and should only affect port "))

(defgeneric disconnect-port (port)
  (:documentation "As I've said before, a port can be connected to multiple
channels at once but since default implementation doesn't support that nor
recommends that, it does not provie method to specify which port to disconnect
from. It's assumed that if this is ever needed the developer will have to
implement such a function himself since this will require writing
channel comparison function which I don't want to do for the sake
keeping this piece of functionality simple."))

(defgeneric send-message (port message &optional async?)
  (:documentation "Sends given messages to port.
Optionally, this function can run in asynchornous fashion (i.e. not block
while sending message)."))

(defgeneric receive-message (port &optional async?)
  (:documentation "Tried to receive message from port.
If ASYNC? is NIL, blocks until it does so. If ASYNC is anything but NIL, tries
to get the message and continutes exectution returning NIL if no data is available
at that time."))

(defgeneric channel= (channel-1 channel-2)
  (:documentation "Checks whether two channels are the same channel.")
  (:method ((channel-1 message-channel) (channel-2 message-channel))
    (uuid:uuid= (slot-value channel-1 'uuid) (slot-value channel-2 'uuid))))

(defgeneric port= (port-1 port-2)
  (:documentation "Checks whether two ports are the same port.")
  (:method ((port-1 message-port) (port-2 message-port))
    (uuid:uuid= (slot-value port-1 'uuid) (slot-value port-2 'uuid))))

(defgeneric port-connected-p (port)
  (:documentation "Checkes whether port is connected to a channel.")
  (:method ((port message-port))
    (with-slots ((port-channel channel)) port
      (atomic port-channel))))

;;; **************************************************************************
;;;  In-memory message transport
;;; **************************************************************************
;;; This is a very simple message transport that stores data in queues and
;;; distributes it using transactional channels. It is preferred message
;;; transport scheme to use when only managing work on single machine.

(define-condition channel-fully-occupied ()
  ((channel :initarg :channel)
   (port-to-attach :initarg :port))
  (:documentation "Raised when port is tried to be connected to a channel that
already has two ports connected."))

(define-condition port-already-connected ()
  ((port :initarg :port))
  (:documentation "Raised when port is already connected to a channel yet
someone tries to connect it again."))

(define-condition port-not-connected ()
  ((port :initarg :port))
  (:documentation "Raised when port is not connected to a channel yet someone
tries to disconnect it."))

(defclass message-channel/local (message-channel)
  ((ports :initform (tc:tcons nil nil))
   (car-queue :initform (make-instance 'tc:transactional-fifo-queue)
              :documentation "CAR of ports seds here. CDR of ports receives from here.")
   (cdr-queue :initform (make-instance 'tc:transactional-fifo-queue)
              :documentation "CDR of ports seds here. CAR of ports receives from here."))
  (:documentation "Local message channel storing data in queues."))

(defclass message-port/local (message-port)
  ((rx-queue :initform nil)
   (tx-queue :initform nil))
  (:documentation "Local port. Used to connect to local channel."))

(defmethod print-object ((instance message-channel/local) stream)
  (with-slots (uuid ports) instance
    (print-unreadable-object (instance stream :type t)
      (format stream "[~A] ~A <-> ~A"
              uuid
              (atomic (tc:tcar ports))
              (atomic (tc:tcdr ports))))))

(defmethod print-object ((instance message-port/local) stream)
  (with-slots (uuid channel) instance
    (print-unreadable-object (instance stream :type t)
      (format stream "[~A] ~A"
              uuid
              (if channel
                "CONNECTED"
                "FLOATING")))))

(defun initialize-local-port-queues (port channel position)
  (check-type position (member :car :cdr))
  (with-slots (rx-queue tx-queue) port
    (with-slots (car-queue cdr-queue) channel
      (atomic (setf rx-queue (case position
                               (:car cdr-queue)
                               (:cdr car-queue))))
      (atomic (setf tx-queue (case position
                               (:car car-queue)
                               (:cdr cdr-queue)))))))

(defun initialize-local-port-channel (port channel)
  (with-slots ((port-channel channel)) port
    (atomic
     ;; Over UNLESS because otherwise I'd need two transactions and I don't
     ;; want two since in most cases there will be two because nobody in right
     ;; mind tries attaching port to a channel two times.
     (if (and port-channel (channel= port-channel channel)) ; In case it's "one way" connection
                                                       ; i.e. not recorded in channel
       (error 'port-already-connected
              :port port))
       (setf port-channel channel))))

(defun clean-local-port-queues (port)
  (with-slots (rx-queue tx-queue) port
    (atomic (setf rx-queue nil))
    (atomic (setf tx-queue nil))))

(defun clean-local-port-channel (port)
  (with-slots (channel) port
    (atomic (setf channel nil))))

(defmethod connect-port ((port message-port/local) (channel message-channel/local))
  ;; Check if port has a connection
  (when (port-connected-p port)
    (error 'port-already-connected
           :port port))
  (with-slots (ports) channel
    (let (car cdr)
      (cond
        ((null (setf car (atomic (tc:tcar ports))))
         (atomic (setf (tc:tcar ports) port))
         (initialize-local-port-queues port channel :car))
        ((null (setf cdr (atomic (tc:tcdr ports))))
         (atomic (setf (tc:tcdr ports) port))
         (initialize-local-port-queues port channel :cdr))
        ((or (and car (port= port car)) (and cdr (port= port cdr)))
         (error 'port-already-connected
                :port port))
        (t
         (error 'channel-fully-occupied
                :channel channel
                :port port)))))
  (initialize-local-port-channel port channel)
  port)

(defmethod disconnect-port ((port message-port/local))
  (unless (port-connected-p port)
    (error 'port-not-connected
           :port port))
  (with-slots (channel) port
    (with-slots (ports) channel
      (let (car cdr)
        (cond
          ((and (setf car (atomic (tc:tcar ports)))
                (port= car port))
           (atomic (setf (tc:tcar ports) nil)))
          ((and (setf cdr (atomic (tc:tcdr ports)))
                (port= cdr port))
           (atomic (setf (tc:tcar ports) nil)))
          (t
           (error 'port-not-connected
                  :port port))))))
  (clean-local-port-queues port)
  (clean-local-port-channel port)
  port)

(defmethod send-message ((port message-port/local) (message message) &optional async?)
  (declare (ignore async?)) ; Ignoring async since pushing to queue is always asynchronous.
  (with-slots (tx-queue) port
    (atomic
     (tc:put message tx-queue))))

(defmethod receive-message ((port message-port/local) &optional async?)
  (with-slots (rx-queue) port
    (if async?
      (atomic
       (tc:try-take rx-queue))
      (atomic
       (tc:take rx-queue)))))
