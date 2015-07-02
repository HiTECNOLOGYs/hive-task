(defpackage #:org.hitecnologys.hive-task
  (:nicknames #:hive-task
              #:ht)
  (:use #:closer-common-lisp
        #:alexandria
        #:stmx)
  (:export #:start-scheduler
           #:stop-scheduler

           #:put-work
           #:make-work))
