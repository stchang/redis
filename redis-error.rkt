#lang racket/base
(provide (all-defined-out))

(struct exn:fail:redis exn:fail ())

(define-syntax-rule (redis-error msg)
  (raise (exn:fail:redis (string-append "redis ERROR: " msg)
                         (current-continuation-marks))))
