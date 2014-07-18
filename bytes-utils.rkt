#lang racket/base

(provide (all-defined-out))

(define CRLF #"\r\n")

(define o compose)
(define number->bytes (o string->bytes/utf-8 number->string))
(define symbol->bytes (o string->bytes/utf-8 symbol->string))
(define bytes->number (o string->number bytes->string/utf-8))
(define bytes->symbol (o string->symbol bytes->string/utf-8))
