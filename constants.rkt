#lang racket/base
(provide (all-defined-out))
(define LOCALHOST "127.0.0.1")
(define DEFAULT-REDIS-PORT 6379)
(define RESET-MSG #"i-am-reset")
;; (define SUBSCRIBE-CMD-UP #"SUBSCRIBE")
;; (define PSUBSCRIBE-CMD-UP #"PSUBSCRIBE")
;; (define SUBSCRIBE-CMD-DOWN #"subscribe")
;; (define PSUBSCRIBE-CMD-DOWN #"psubscribe")
