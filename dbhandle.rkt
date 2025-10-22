(require db)

(define (make-db-connection!)
  (sqlite3-connect #:database "lib/farmfind.db"))

