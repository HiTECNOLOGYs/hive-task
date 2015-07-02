(defsystem #:hive-task
  :author "Mark Fedurin <hitecnologys@gmail.com>"
  :description "STM-based concurrency for lazy."
  :depends-on (#:alexandria ; General toolkit
               #:stmx       ; Multithreading
               #:closer-mop ; MOP
               )
  :serial t
  :pathname "src/"
  :components ((:file "packages")
               (:file "utilities")
               (:file "threads")
               (:file "timers")
               (:file "scheduler")))
