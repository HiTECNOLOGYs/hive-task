(defpackage #:org.hitecnologys.transactional-containers
  (:nicknames #:transactional-containers
              #:tc)
  (:use #:closer-common-lisp
        #:alexandria
        #:stmx)

  ;; Generic container methods
  (:export #:full-p  ; ......................... Generic function
           #:empty-p ; ......................... Generic function
           #:empty!  ; ......................... Generic function
           )

  ;; Ordered container methods
  (:export #:peek     ; ........................ Generic function
           #:take     ; ........................ Generic function
           #:put      ; ........................ Generic function
           #:try-take ; ........................ Generic function
           #:try-put  ; ........................ Generic function
           )

  ;; Key-value container methods
  (:export #:entry-count     ; ................. Generic function
           #:get-value       ; ................. Generic function
           #:set-value       ; ................. Generic function
           #:rem-value       ; ................. Generic function
           #:map-container   ; ................. Generic function
           #:copy-container  ; ................. Generic function
           #:container-keys  ; ................. Generic function
           #:container-value ; ................. Generic function
           #:container-pairs ; ................. Generic function
           )

  ;; Transactional container classes
  (:export #:transactional-container           ; Class
           #:transactional-ordered-container   ; Class
           #:transactional-key-value-container ; Class
           #:transactional-cell                ; Class
           #:transactional-filo-queue          ; Class
           #:transactional-fifo-queue          ; Class
           #:transactional-channel             ; Class
           #:transactional-port                ; Class
           #:transactional-hash-table          ; Class
           #:transactional-map                 ; Class
           )

  ;; Additional container-specific methods
  (:export #:transactional-map-predicate ; ..... Generic function
           )
  )

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
