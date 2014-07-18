#lang racket
(require "redis.rkt")
(require rackunit)

;; connection pool concurrency tests

(define p (make-connection-pool))

;; check that owner properly set if connection is released-then-leased quickly
(define conn (connection-pool-lease p))
(check-eq? (redis-connect-owner conn) (current-thread))
(connection-pool-return p conn)
(check-false (redis-connect-owner conn))
(sleep 0.1)
(define conn2 (connection-pool-lease p))
(check-eq? (redis-connect-owner conn2) (current-thread))

;; ideally conn and conn2 should be equal but sometimes it's not
;; since cleanup is not done before next lease request
;; but they are eq here because of the sleep
;; - loop test below will check for starvation
(check-eq? conn conn2)

;; double return should be ok
(check-not-exn (lambda () (connection-pool-return p conn2)))
(check-false (redis-connect-owner conn2))
(check-not-exn (lambda () (connection-pool-return p conn2)))

;; check for starvation, ie that connections are actually getting returned
;; max connections should not be hit
(check-not-exn
  (lambda ()
    (let loop ([n 1000])
      (unless (zero? n)
        (connection-pool-return p (connection-pool-lease p))
        (loop (sub1 n))))))

(check-not-exn (lambda () (kill-connection-pool p)))
(sleep 2) ; need this otherwise program shutdown takes over and checks get ignored
