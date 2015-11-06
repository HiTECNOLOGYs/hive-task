(defsystem #:hive-task
  :author "Mark Fedurin <hitecnologys@gmail.com>"
  :description "STM-based concurrency for lazy."
  :depends-on (#:alexandria               ; General toolkit
               #:com.informatimago.clmisc ; More tools
               #:stmx                     ; Multithreading
               #:closer-mop               ; MOP
               #:uuid                     ; Used for identifying various entities
               )
  :serial t
  :pathname "src/"
  :components ((:file "packages")
               (:file "utilities")
               (:file "primitives")
               (:file "threads")
               (:file "timers")
               (:file "message-transport")
               (:file "scheduler")))
