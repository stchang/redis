#lang racket

; redis bindings for racket

; based on protocol decribed here: http://redis.io/topics/protocol

;; TODO:
;; [x] 2013-09-25: make redis-specific exn
;;     DONE: 2013-09-26
;; [x] 2013-09-25: dont automatically set rconn parameter on connect
;;                 - makes it hard to use parameterize with (connect)
;;     DONE: 2013-09-26

(provide connect disconnect send-cmd current-redis-connection
         exn:fail:redis? get-reply)

;; connect/disconnect ---------------------------------------------------------
(struct redis-connection (in out))

(define current-redis-connection (make-parameter #f))

(define (connect #:host [host "127.0.0.1"] #:port [port 6379])
  (define-values (in out) (tcp-connect host port))
  (redis-connection in out))

(define (disconnect [rconn (current-redis-connection)])
  (match-define (redis-connection in out) rconn)
  (send-cmd #:rconn rconn "QUIT")
  (close-input-port in) (close-output-port out))

;; send cmd/recv reply --------------------------------------------------------
(define CRLF #"\r\n")
(struct exn:fail:redis exn:fail ())

(define (send-cmd #:rconn [rconn (current-redis-connection)] cmd . args)
  (match-define (redis-connection in out) rconn)
  (write-bytes (mk-request cmd args) out)
  (flush-output out)
  ;; must catch and re-throw here to display offending cmd and args
  (with-handlers ([exn:fail?
                   (lambda (x)
                     (raise (exn:fail:redis
                             (format "~a\nCMD: ~a\nARGS: ~a\n"
                                     (exn-message x) cmd args)
                             (current-continuation-marks))))])
    (get-reply in)))

(define (mk-request cmd args)
  (bytes-append
   (string->bytes/utf-8
    (string-append "*" (number->string (add1 (length args))) "\r\n"))
   (arg->bytes cmd)
   (apply bytes-append (map arg->bytes args))))

(define (number->bytes n) (string->bytes/utf-8 (number->string n)))
(define (symbol->bytes x) (string->bytes/utf-8 (symbol->string x)))
(define (arg->bytes val)
  (define bs
    (cond [(bytes? val) val]
          [(string? val) (string->bytes/utf-8 val)]
          [(number? val) (number->bytes val)]
          [(symbol? val) (symbol->bytes val)]
          [else (error 'send "invalid argument: ~v\n" val)]))
  (bytes-append #"$" (number->bytes (bytes-length bs)) CRLF bs CRLF))

;; cmd and args are used for error reporting
(define (get-reply [in (redis-connection-in (current-redis-connection))])
  (define byte1 (read-char in))
  (define reply1 (read-line in 'return-linefeed))
  (match byte1
    [#\+ reply1] ; Status reply
    [#\- (error 'redis-reply reply1)] ; Error reply
    [#\: (string->number reply1)]     ; Integer reply
    [#\$ (let ([numbytes (string->number reply1)])   ; Bulk reply
           (if (= numbytes -1) #\null
               (begin0
                 (read-bytes numbytes in)
                 (read-line in 'return-linefeed))))]
    [#\* (let ([numreplies (string->number reply1)]) ; Multi-bulk reply
           (if (= numreplies -1) #\null
               (for/list ([n (in-range numreplies)]) (get-reply in))))]))


