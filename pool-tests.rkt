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

;; check that connections established in a separate custodian are not closed
;; when the custodian is shut down
(define pool-cust-test (make-connection-pool))
(define a-cust (make-custodian))
(parameterize ([current-redis-pool pool-cust-test])
  (check-not-exn
    (lambda ()
      (parameterize ([current-custodian a-cust])
        (connection-pool-return (connection-pool-lease)))
      (custodian-shutdown-all a-cust)
      (sleep 0.1)
      (connection-pool-return (connection-pool-lease)))))

(define pool-max-test (make-connection-pool))
;; test max connections by making a lot of threads
(parameterize ([current-redis-pool pool-max-test])
  (define exn-chan (make-async-channel))
  (define threads
    (for/list ([n (in-range 0 101)])
      (thread (lambda ()
        (with-handlers ([exn:fail:redis? (lambda _ (async-channel-put exn-chan 'fail))])
          (connection-pool-lease))
        (thread-suspend (current-thread))))))
  (sleep 3) ; wait for threads to be created
  (check-eq? (async-channel-try-get exn-chan) 'fail)
  (for ([thread threads])
    (kill-thread thread)))

;; torture test
;; spawn 50 threads that each repeat many times:
;;   ECHO a random number, check the result,
;;   and 1/4 of the time SUBSCRIBE to a channel just before
;;   returning the connection to the pool
(parameterize ([current-redis-pool p])
  (check-not-exn
    (lambda ()
      (define threads
        (for/list ([n (in-range 1 50)])
          (thread
            (thunk
              (for/list ([k (in-range 0 (random 2000))])
                (let ([c (random 100)])
                  (unless
                    (= c
                       (string->number
                         (bytes->string/utf-8
                           (send-cmd "ECHO" c))))
                    (error 'torture "mismatch"))
                  (when (< c 25)
                    (send-cmd "SUBSCRIBE" (~a "asdf" c))
                    (connection-pool-return (connection-pool-lease)))))))))
      (for ([thread threads])
        (sync (thread-dead-evt thread))))))

(check-not-exn (lambda () (kill-connection-pool p)))
(sleep 1) ; need this otherwise program shutdown takes over and checks get ignored
(check-not-exn (lambda () (kill-connection-pool pool-cust-test)))
(sleep 1) ; need this otherwise program shutdown takes over and checks get ignored
(check-not-exn (lambda () (kill-connection-pool pool-max-test)))
(sleep 1) ; need this otherwise program shutdown takes over and checks get ignored
