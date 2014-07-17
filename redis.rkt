#lang racket/base

; redis bindings for racket

; based on protocol decribed here: http://redis.io/topics/protocol

;; TODO:
;; [x] 2013-09-25: make redis-specific exn
;;     DONE: 2013-09-26
;; [x] 2013-09-25: dont automatically set rconn parameter on connect
;;                 - makes it hard to use parameterize with (connect)
;;     DONE: 2013-09-26

(require "bytes-utils.rkt"
         racket/tcp
         racket/match
         racket/async-channel
         racket/contract)
(provide connect disconnect send-cmd send-cmd/no-reply current-redis-connection
         exn:fail:redis? get-reply get-reply-evt with-redis-connection 
         redis-connection? make-connection-pool kill-connection-pool)

;; connect/disconnect ---------------------------------------------------------
(struct redis-connection ())
(struct redis-connection-single redis-connection 
        (in out pool [owner #:mutable]))
(struct redis-connection-pool redis-connection
  (host port [dead? #:mutable] key=>conn
        idle-conns fresh-conn-sema manager-thread))

(define current-redis-connection (make-parameter #f))

(struct exn:fail:redis exn:fail ())

(define-syntax-rule (redis-error msg)
  (raise (exn:fail:redis (string-append "redis ERROR: " msg)
                         (current-continuation-marks))))

;; Construct a redis-connection-pool that will lease at most max-connections
;; to client threads and will maintain at most max-idle unleased connections.
;;
;; A connection pool will lease a connection to a client thread when (connect)
;; is called on the pool. Further calls to (connect) on the same connection pool
;; and in the same thread, with no intervening calls to (disconnect), will
;; produce the same connection. No other thread will be able to use the
;; connection until it is returned to the pool.
;;
;; Leased connections are explicitly returned to the pool of available
;; connections when (disconnect) is called on the connection pool whence
;; the connection was leased or on the leased connection itself. Leased
;; connections are also returned to the pool when the leasing thread dies.
;;
;; Calling any command on a connection that is not leased to the current thread
;; will raise an exception. Attepting to lease a connection when there are no
;; available connections in the pool will block until a connection is available.
;;
;; A connection pool can be killed with kill-connection-pool. Killing a
;; connection pool causes it to lease no further connections, close all idle
;; connections, and immediately close any connection that is returned to the
;; pool. Attempting to lease from a dead connection pool will result in an
;; exception. Threads that were blocked on leasing a connection from a
;; connection pool that was killed will block forever.
(define/contract
  (make-connection-pool #:host [host "127.0.0.1"]
                        #:port [port 6379]
                        #:max-connections [max-conn 100]
                        #:max-idle [max-idle 10])
  (->* () (#:host
           string?
           #:port
           exact-nonnegative-integer?
           #:max-connections
           exact-nonnegative-integer?
           #:max-idle
           exact-nonnegative-integer?)
       redis-connection-pool?)
  (define idle-return-chan (make-async-channel max-idle))
  (define key=>conn (make-hasheq))
  (define fresh-conn-sema (make-semaphore max-conn))
  (define (release-conn thd.conn dead?)
    (match-define (cons thd conn) thd.conn)
    (set-redis-connection-single-owner! conn (current-thread))
    (hash-remove! key=>conn thd)
    (if dead?
        (disconnect conn)
        (begin
          ;; Reset state of returned connection.
          (send-cmd/no-reply #:rconn conn "UNSUBSCRIBE")
          (send-cmd/no-reply #:rconn conn "UNWATCH")
          (send-cmd/no-reply #:rconn conn "ECHO" #"i-am-reset")
          (let loop ()
            (unless (equal? (get-reply (redis-connection-single-in conn)) #"i-am-reset")
              (loop)))
          (unless (sync/timeout 0 (async-channel-put-evt idle-return-chan conn))
            (disconnect conn)
            (semaphore-post fresh-conn-sema))))
    (set-redis-connection-single-owner! conn #f))
  (define pool
    (redis-connection-pool
     host port #f key=>conn idle-return-chan fresh-conn-sema
     (thread
      (lambda()
        (let loop ()
          (sync
           (handle-evt
            (apply choice-evt
                   (map (lambda(thd.conn) (wrap-evt (thread-dead-evt (car thd.conn)) (lambda(_) thd.conn)))
                        (hash->list key=>conn)))
            (lambda(thd.conn)
              (when (redis-connection-single-owner (cdr thd.conn))
                (release-conn thd.conn (redis-connection-pool-dead? pool)))
              (loop)))
           (handle-evt
            (thread-receive-evt)
            (lambda(_)
              (match (thread-receive)
                ['die
                 (set-redis-connection-pool-dead?! pool #t)
                 (let dis-loop ()
                   (let ([maybe-idle-conn (async-channel-try-get idle-return-chan)])
                     (when maybe-idle-conn
                       (disconnect maybe-idle-conn)
                       (dis-loop))))
                 (loop)]
                ['new (loop)]
                [thd.conn
                 (release-conn thd.conn (redis-connection-pool-dead? pool))
                 (loop)])))))))))
  pool)

(define (kill-connection-pool pool)
  (thread-send (redis-connection-pool-manager-thread pool) 'die))

(define (connection-pool-lease pool)
  (when (redis-connection-pool-dead? pool)
    (redis-error "Attempted to lease connection from dead connection pool."))
  (or (hash-ref (redis-connection-pool-key=>conn pool) (current-thread) #f)
      (let ([conn
             (or (async-channel-try-get (redis-connection-pool-idle-conns pool))
                 (sync (redis-connection-pool-idle-conns pool)
                       (wrap-evt (redis-connection-pool-fresh-conn-sema pool)
                                 (lambda(_)
                                   (real-connect (redis-connection-pool-host pool)
                                                 (redis-connection-pool-port pool)
                                                 pool)))))])
        (hash-set! (redis-connection-pool-key=>conn pool) (current-thread) conn)
        (set-redis-connection-single-owner! conn (current-thread))
        (thread-send (redis-connection-pool-manager-thread pool) 'new)
        conn)))

(define (connection-pool-return pool conn)
  (unless (eq? (current-thread) (redis-connection-single-owner conn))
    (redis-error "Attempted to disconnect leased connection when not owner."))
  ;; prevent double-return
  (set-redis-connection-single-owner! conn #f)
  (thread-send (redis-connection-pool-manager-thread pool)
               (cons (current-thread) conn)))

(define/contract (connect #:host [host "127.0.0.1"] #:port [port 6379])
  (->* () (#:host string? #:port exact-nonnegative-integer?) redis-connection?)
  (let ([rconn (current-redis-connection)])
    (if (and (redis-connection-pool? rconn)
             (string=? (redis-connection-pool-host rconn) host)
             (= (redis-connection-pool-port rconn) port))
        (connection-pool-lease rconn)
        (real-connect host port #f))))

(define (real-connect host port pool)
  (let-values ([(in out) (tcp-connect host port)])
    (redis-connection-single in out pool (current-thread))))

(define/contract (disconnect [rconn (current-redis-connection)])
  (->* () (redis-connection?) void?)
  (match rconn
    [#f (redis-error "Can't disconnect when not connected to server.")]
    [(redis-connection-single _ _ (? redis-connection-pool? pool) _)
     (disconnect pool)]
    [(redis-connection-single in out _ _)
     (send-cmd #:rconn rconn "QUIT")
     (close-input-port in) (close-output-port out)]
    [(redis-connection-pool _ _ _ key=>conn _ _ _)
     (let ([maybe-connection (hash-ref key=>conn (current-thread) #f)])
       (when (and maybe-connection (eq? (current-thread) (redis-connection-single-owner maybe-connection)))
         (connection-pool-return rconn maybe-connection)))]))

(define-syntax-rule (with-redis-connection e0 e ...)
  (let ([rconn (connect)])
    (parameterize ([current-redis-connection rconn])
      (dynamic-wind
       (lambda() (void))
       (lambda() e0 e ...)
       (lambda() (disconnect))))))

;; send cmd/recv reply --------------------------------------------------------
(define CRLF #"\r\n")

(define (send-cmd/no-reply #:rconn [conn (current-redis-connection)] cmd . args)
  (define rconn (if (redis-connection-single? conn) conn (connect)))
  (match-define (redis-connection-single in out _ owner) rconn)
  (unless (eq? (current-thread) owner)
    (redis-error "Attempted to use redis connection in thread other than owner."))
  (write-bytes (mk-request cmd args) out)
  (flush-output out))

(define (send-cmd #:rconn [conn (current-redis-connection)] cmd . args)
  (define rconn (if (redis-connection-single? conn) conn (connect)))
  (match-define (redis-connection-single in out _ owner) rconn)
  (unless (eq? (current-thread) owner)
    (redis-error "Attempted to use redis connection in thread other than owner."))
  (write-bytes (mk-request cmd args) out)
  (flush-output out)
  ;; must catch and re-throw here to display offending cmd and args
  (with-handlers ([exn:fail?
                   (lambda (x)
                     (redis-error (format "~a\nCMD: ~a\nARGS: ~a\n"
                                          (exn-message x) cmd args)))])
    (begin0 (get-reply in) (unless conn (disconnect rconn)))))

(define (mk-request cmd args)
  (bytes-append
   (string->bytes/utf-8
    (string-append "*" (number->string (add1 (length args))) "\r\n"))
   (arg->bytes cmd)
   (apply bytes-append (map arg->bytes args))))

(define (arg->bytes val)
  (define bs
    (cond [(bytes? val) val]
          [(string? val) (string->bytes/utf-8 val)]
          [(number? val) (number->bytes val)]
          [(symbol? val) (symbol->bytes val)]
          [else (error 'send "invalid argument: ~v\n" val)]))
  (bytes-append #"$" (number->bytes (bytes-length bs)) CRLF bs CRLF))

;; cmd and args are used for error reporting
(define (get-reply [in (redis-connection-single-in (current-redis-connection))])
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

(define (get-reply-evt [in (redis-connection-single-in (current-redis-connection))])
  (wrap-evt in (lambda(_) (get-reply in))))
