#lang racket
(require "redis.rkt" "redis-error.rkt" "test-utils.rkt")
(require rackunit racket/async-channel)

;; connection pool concurrency tests

(define p (make-connection-pool))

;; check that owner properly set if connection is released-then-leased quickly
(define conn (connection-pool-lease p))
(check-eq? (redis-connection-owner conn) (current-thread))
(connection-pool-return conn p)
(check-false (redis-connection-owner conn))
(sleep 0.01)
(define conn2 (connection-pool-lease p))
(check-eq? (redis-connection-owner conn2) (current-thread))

;; ideally conn and conn2 should be equal but sometimes it's not
;; since cleanup is not done before next lease request
;; so sleep to give old conn time to cleanup
(check-eq? conn conn2)

;; double return should be ok
(check-not-exn (lambda () (connection-pool-return conn2 p)))
(check-false (redis-connection-owner conn2))
(check-not-exn (lambda () (connection-pool-return conn2 p)))

;; check for starvation, ie that connections are actually getting returned
;; max connections should not be hit; timeout should *not* be triggered
(check-not-exn
  (lambda ()
    (let loop ([n 1000])
      (unless (zero? n)
        (connection-pool-return (connection-pool-lease p) p)
        (loop (sub1 n))))))

(define pool-max-test (make-connection-pool))
;; test max connections by making a lot of threads
(parameterize ([current-redis-pool pool-max-test])
  (define exn-chan (make-async-channel))
  (let loop ([n 101])
    (unless (zero? n)
      (thread (lambda () 
        (with-handlers ([exn:fail:redis? (lambda _ (async-channel-put exn-chan 'fail))])
          (connection-pool-lease))
        (let loop () (loop))))
      (loop (sub1 n))))
  (sleep 2) ; wait for threads to be created
  (check-eq? (async-channel-try-get exn-chan) 'fail))

(check-not-exn (lambda () (kill-connection-pool p)))
(sleep 1) ; need this otherwise program shutdown takes over and checks get ignored
(check-not-exn (lambda () (kill-connection-pool pool-max-test)))
(sleep 1) ; need this otherwise program shutdown takes over and checks get ignored
