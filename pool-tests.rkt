#lang racket
(require "redis.rkt" "redis-cmds.rkt")

;; connection pool concurrency tests

(module+ test
  (require rackunit
           rackunit/text-ui)
  (define p (make-connection-pool))
  (current-redis-pool p)
  
  (define-test-suite pool-tests
    (test-begin
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
     (check-not-exn (lambda () (connection-pool-return p conn2))))
    
    ;; check for starvation, ie that connections are actually getting returned
    ;; max connections should not be hit
    (check-not-exn
     (lambda ()
       (let loop ([n 10000]
                  [s (for/seteq ([i (in-range 1 10)])
                                (connection-pool-lease p))])
         (if (zero? n)
             (for ([conn s]) (connection-pool-return p conn))
             (begin
               (connection-pool-return p (set-first s))
               (loop (sub1 n)
                     (set-add (set-rest s) (connection-pool-lease p))))))))
    
    ;; torture test
    (check-not-exn
     (lambda ()
       (define threads
         (for/list ([n (in-range 1 50)])
           (thread
            (thunk
             (for/list ([k (in-range 0 (random 2000))])
               (let ([c (random 100)])
                 (unless (= (string->number (bytes->string/utf-8 (ECHO c))) c)
                   (error 'torture "mismatch"))
                 (when (< c 25)
                   (SUBSCRIBE (~a "asdf" c))
                   (connection-pool-return p (connection-pool-lease p)))))))))
       (for ([thread threads])
         (sync (thread-dead-evt thread)))))
    
    (check-not-exn (lambda () (kill-connection-pool p))))
  
  (void (run-tests pool-tests)))
