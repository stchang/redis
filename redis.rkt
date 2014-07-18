#lang racket/base

; redis bindings for racket

; based on protocol decribed here: http://redis.io/topics/protocol

;; TODO:
;; [x] 2013-09-25: make redis-specific exn
;;     DONE: 2013-09-26
;; [x] 2013-09-25: dont automatically set rconn parameter on connect
;;                 - makes it hard to use parameterize with (connect)
;;     DONE: 2013-09-26

(require "bytes-utils.rkt" "constants.rkt"
         racket/tcp racket/match racket/async-channel racket/contract)
(provide connect disconnect send-cmd send-cmd/no-reply current-redis-pool
         exn:fail:redis? get-reply get-reply-evt connection-pool-lease
         connection-pool-return redis-connect-owner
         redis-connect? make-connection-pool kill-connection-pool)

;; connect/disconnect ---------------------------------------------------------
;; owner = thread or #f; #f = pool thread owns it
(struct redis-connect (in out [owner #:mutable]))
(struct redis-connection-pool 
  (host port [dead? #:mutable] 
   key=>conn  ; maps thread to its connection
   idle-conns ; queue (async-chan) of avail connections
   fresh-conn-sema ; checks connection limit
   manager-thread))

(define current-redis-pool (make-parameter #f))

(struct exn:fail:redis exn:fail ())

(define-syntax-rule (redis-error msg)
  (raise (exn:fail:redis (string-append "redis ERROR: " msg)
                         (current-continuation-marks))))

(define-syntax-rule (check-owner-ok connection operation-str)
  (let ([curr-owner (redis-connect-owner connection)])
    (unless (or (not curr-owner) ; owner = #f means the pool has it, so it's ok
                (eq? curr-owner (current-thread)))
      (redis-error 
        (format "Attempted to ~a connection when not owner." operation-str)))))

  

;; Construct a redis-connection-pool that will lease at most max-connections
;; to client threads and will maintain at most max-idle unleased connections.
;;
;; A connection pool will lease a connection to a client thread when 
;; connection-pool-lease is called on the pool. Further calls to 
;; connection-pool-lease on the same connection pool and in the same thread, 
;; with no intervening calls to connection-pool-return, will produce the 
;; same connection. No other thread will be able to use the connection until 
;; it is returned to the pool.
;;
;; Leased connections are explicitly returned to the pool of available
;; connections when connection-pool-return is called on the connection pool 
;; whence the connection was leased. Leased connections are also returned to 
;; the pool when the leasing thread dies.
;;
;; Calling any command on a connection that is not leased to the current thread
;; will raise an exception (except in the case of the pool's thread, which is 
;; given elevated privilege). Attepting to lease a connection when there are no
;; available connections in the pool will block until a connection is 
;; available.
;;
;; A connection pool can be killed with kill-connection-pool. Killing a
;; connection pool causes it to lease no further connections, close all idle
;; connections, and immediately close any connection that is returned to the
;; pool. Attempting to lease from a dead connection pool will result in an
;; exception. Threads that were blocked on leasing a connection from a
;; connection pool that was killed will block forever.
(define/contract
  (make-connection-pool #:host [host LOCALHOST]
                        #:port [port DEFAULT-REDIS-PORT]
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
  ;; using async-channel here as a concurrent queue
  (define idle-return-chan (make-async-channel max-idle))
  ;; key (ie thread) \in hashtable == connection is leased out
  ;; NOTE: The hash table determines who has the connection.
  ;;       The "owner" field in a connection struct is just a secondary check
  (define key=>conn (make-hasheq))
  (define fresh-conn-sema (make-semaphore max-conn))
  (define (release-conn thd.conn dead?)
    (match-define (cons thd conn) thd.conn)
    (hash-remove! key=>conn thd)
    (set-redis-connect-owner! conn #f)
    (cond ; if pool is dead, disconnect from db, ow cleanup and return to pool
     [dead? (disconnect conn)]
     [else
      ;; Reset state of returned connection.
;      (send-cmd/no-reply #:rconn conn "UNSUBSCRIBE")
;      (send-cmd #:rconn conn "UNWATCH")
;      (send-cmd/no-reply #:rconn conn "ECHO" RESET-MSG)
#;      (let loop ()
        (unless (eq? (get-reply (redis-connect-in conn)) RESET-MSG) (loop)))
      (unless (sync/timeout 0 (async-channel-put-evt idle-return-chan conn))
        (disconnect conn)
        (semaphore-post fresh-conn-sema))]))
  (define pool-thread (thread (lambda ()
    (let loop ()
      (sync
        ;; release connection if owner thread dies
        (handle-evt
          (apply choice-evt
            (map 
             (lambda (thd.conn) 
               (wrap-evt (thread-dead-evt (car thd.conn)) (lambda _ thd.conn)))
             (hash->list key=>conn)))
          (lambda (thd.conn)
            (when (redis-connect-owner (cdr thd.conn))
              (release-conn thd.conn (redis-connection-pool-dead? pool)))
            (loop)))
        ;; handle thread msgs
        (handle-evt
          (thread-receive-evt)
          (lambda _
            (match (thread-receive)
             ['die
              (set-redis-connection-pool-dead?! pool #t)
              (let dis-loop ()
                (let ([maybe-idle-conn 
                       (async-channel-try-get idle-return-chan)])
                  (when maybe-idle-conn
                    (disconnect maybe-idle-conn)
                    (dis-loop))))
              (loop)]
             ['new (loop)]
             [thd.conn
              (release-conn thd.conn (redis-connection-pool-dead? pool))
              (loop)]))))))))
  (define pool
    (redis-connection-pool
      host port #f key=>conn idle-return-chan fresh-conn-sema pool-thread))
  pool)

(define (kill-connection-pool pool)
  (thread-send (redis-connection-pool-manager-thread pool) 'die))

(define (connection-pool-lease pool)
  (when (redis-connection-pool-dead? pool)
    (redis-error "Attempted to lease connection from dead connection pool."))
  (or 
   ;; if curr-thread already has a connection, return the same connection
   ;; ie, 1 connection per thread
   (hash-ref (redis-connection-pool-key=>conn pool) (current-thread) #f)
   ;; else get from avail connections
   (let ([conn
          (or (async-channel-try-get (redis-connection-pool-idle-conns pool))
              ;; if no conns avail:
              ;; - either connection becomes available in the queue,
              ;; - or create a new connection
              (sync/timeout 2
                (redis-connection-pool-idle-conns pool)
                (wrap-evt (redis-connection-pool-fresh-conn-sema pool)
                  (lambda _
                    (connect (redis-connection-pool-host pool)
                             (redis-connection-pool-port pool))))))])
     (unless conn (redis-error "Maximum connections reached."))
     (hash-set! (redis-connection-pool-key=>conn pool) (current-thread) conn)
     (set-redis-connect-owner! conn (current-thread))
     (thread-send (redis-connection-pool-manager-thread pool) 'new)
     conn)))

(define (connection-pool-return pool conn)
  (check-owner-ok conn "RETURN")
  ;; this check prevents double returns by the same thread;
  ;; shouldn't get race condition between two different threads since they
  ;; cant own/return the same connection
  (when (hash-has-key? (redis-connection-pool-key=>conn pool) (current-thread))
    (hash-remove! (redis-connection-pool-key=>conn pool) (current-thread))
    (set-redis-connect-owner! conn #f)
    (thread-send (redis-connection-pool-manager-thread pool)
                 (cons (current-thread) conn))))

(define/contract (connect host port)
  (-> string? exact-nonnegative-integer? redis-connect?)
  (let-values ([(in out) (tcp-connect host port)])
    (redis-connect in out (current-thread))))

(define/contract (disconnect conn)
  (-> redis-connect? void?)
  (match conn
    [(redis-connect in out _)
     (send-cmd #:rconn conn "QUIT")
     (close-input-port in) (close-output-port out)]))

#;(define-syntax-rule (with-redis-connection e0 e ...)
  (let ([rconn (connect)])
    (parameterize ([current-redis-connection rconn])
      (dynamic-wind
       (lambda() (void))
       (lambda() e0 e ...)
       (lambda() (disconnect))))))

;; send cmd/recv reply --------------------------------------------------------

;; - Use conn if provided.
;; - If no conn, then get it from the current pool.
;; - If current pool not set, then create it (and set as current) and get conn.
(define (send-cmd/no-reply #:rconn [conn #f]
                           #:host [host LOCALHOST]
                           #:port [port DEFAULT-REDIS-PORT] 
                           cmd . args)
  (define rconn 
    (or conn 
        (connection-pool-lease
          (or (current-redis-pool) 
              (let ([new-pool (make-connection-pool #:host host #:port port)])
                (current-redis-pool new-pool)
                new-pool)))))
  (match-define (redis-connect in out owner) rconn)
  (check-owner-ok rconn (format "SEND-CMD (~a: ~a) on" cmd args))
  (write-bytes (mk-request cmd args) out)
  (flush-output out)
  rconn)

(define (send-cmd #:rconn [conn #f] 
                  #:host [host LOCALHOST]
                  #:port [port DEFAULT-REDIS-PORT] 
                  cmd . args)
  (define rconn 
    (apply send-cmd/no-reply #:rconn conn #:host host #:port port cmd args))
  ;; must catch and re-throw here to display offending cmd and args
  (with-handlers ([exn:fail?
                   (lambda (x)
                     (redis-error (format "~a\nCMD: ~a\nARGS: ~a\n"
                                          (exn-message x) cmd args)))])
    (get-reply (redis-connect-in rconn))))

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
(define (get-reply [in (redis-connect-in
                         (connection-pool-lease (current-redis-pool)))])
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

(define (get-reply-evt [in (redis-connect-in 
                             (connection-pool-lease (current-redis-pool)))])
  (wrap-evt in get-reply))
