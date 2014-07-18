#lang racket
(require "redis.rkt" "redis-cmds.rkt")
(require rackunit)
(provide (all-defined-out))

(define-syntax-rule (test tst ...)
  (parameterize ([current-redis-pool (make-connection-pool)])
    (let* ([keys (KEYS "*")]
           [old (map (lambda (x) (DUMP x)) keys)])
      (for-each (lambda (x) (check-equal? (DEL x) 1)) keys)
      tst ...
      (for-each (lambda (k v) (DEL k) (RESTORE k 0 v)) keys old))
    (kill-connection-pool (current-redis-pool))))

(define-syntax-rule (check-redis-exn e)
  (check-exn exn:fail:redis? (lambda () e)))

(define-syntax-rule (check-set-equal? e (x ...))
  (check-equal? (list->set e) (set x ...)))

(define-syntax-rule (check-void? e) (check-equal? e (void)))
