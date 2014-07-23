#lang racket/base

; redis bindings for racket

; based on protocol described here: http://redis.io/topics/protocol

;; TODO:
;; [x] 2013-09-25: make redis-specific exn
;;     DONE: 2013-09-26
;; [x] 2013-09-25: dont automatically set rconn parameter on connect
;;                 - makes it hard to use parameterize with (connect)
;;     DONE: 2013-09-26

(require (for-syntax racket/base syntax/parse)
         racket/tcp racket/match 
         racket/async-channel racket/contract racket/port
         "redis-error.rkt" "bytes-utils.rkt" "constants.rkt")
(provide connect disconnect (struct-out redis-connection)
         send-cmd send-cmd/no-reply current-redis-pool
         get-reply get-reply/port get-reply-evt get-reply-evt/port
         make-connection-pool kill-connection-pool
         connection-pool-lease connection-pool-return 
         pubsub-connection? lease-pubsub-conn return-pubsub-conn make-subscribe-chan)

;; connect/disconnect ---------------------------------------------------------
;; owner = thread or #f; #f = pool thread owns it
(struct redis-connection (in out [owner #:mutable] [pubsub? #:auto #:mutable]))

(define/contract (connect [host LOCALHOST] [port DEFAULT-REDIS-PORT])
  (->* () (string? (integer-in 1 65535)) redis-connection?)
  (define-values (in out) (tcp-connect host port))
  (redis-connection in out (current-thread)))

(define/contract (disconnect conn)
  (-> redis-connection? void?)
  (send-cmd #:rconn conn "QUIT")
  (match-define (redis-connection in out _ _) conn)
  (close-input-port in) (close-output-port out))

;; connection pool ------------------------------------------------------------
(struct redis-connection-pool 
  (host port [dead? #:mutable] 
   key=>conn  ; maps thread to its (leased) connection
   pubsubs    ; leased pubsub connections
   idle-conns ; queue (async-chan) of avail (ie returned) connections
   fresh-conn-sema ; checks connection limit
   manager-thread))

(define current-redis-pool (make-parameter #f))

(define-syntax-rule (check-owner-ok connection operation-str)
  (let ([curr-owner (redis-connection-owner connection)])
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
  ;; pubsubs: [HashEqof redis-connection => Custodian]
  (define pubsubs (make-hasheq))
  (define fresh-conn-sema (make-semaphore max-conn))
  (define (release-conn thd.conn dead?)
    (match-define (cons thd conn) thd.conn)
    (hash-remove! pubsubs conn)
    (hash-remove! key=>conn thd)
    (set-redis-connection-pubsub?! conn #f)
    (set-redis-connection-owner! conn #f)
    (cond ; if pool is dead, disconnect from db, ow cleanup and return to pool
     [dead? (disconnect conn)]
     [else
      ;; Reset state of returned connection.
      (send-cmd/no-reply #:rconn conn "UNWATCH")
      (send-cmd/no-reply #:rconn conn "UNSUBSCRIBE")
      (send-cmd/no-reply #:rconn conn "ECHO" RESET-MSG)
      (let loop () (unless (equal? (get-reply conn) RESET-MSG) (loop)))
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
            (when (redis-connection-owner (cdr thd.conn))
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
      host port #f key=>conn pubsubs
      idle-return-chan fresh-conn-sema pool-thread))
  pool)

(define (kill-connection-pool pool)
  (thread-send (redis-connection-pool-manager-thread pool) 'die))

;; gets an idle connection from the pool, or else makes a new connection
(define (get-idle-or-new-conn pool)
  ;; try to get an idle conn
  (or (async-channel-try-get (redis-connection-pool-idle-conns pool))
      ;; if no conns avail:
      ;; - either connection becomes available in the queue,
      ;; - or create a new connection
      (sync/timeout 2
        (redis-connection-pool-idle-conns pool)
        (wrap-evt (redis-connection-pool-fresh-conn-sema pool)
          (lambda _
            (connect (redis-connection-pool-host pool)
                     (redis-connection-pool-port pool)))))
      (redis-error "Maximum connections reached.")))

(define (connection-pool-lease [pool (current-redis-pool)])
  (unless pool (redis-error "pool-lease: no pool given"))
  (when (redis-connection-pool-dead? pool)
    (redis-error "Attempted to lease connection from dead connection pool."))
  (or 
   ;; if curr-thread already has a connection, return the same connection
   ;; ie, 1 connection per thread
   (hash-ref (redis-connection-pool-key=>conn pool) (current-thread) #f)
   ;; else get from avail connections
   (let ([conn (get-idle-or-new-conn pool)])
     (hash-set! (redis-connection-pool-key=>conn pool) (current-thread) conn)
     (set-redis-connection-owner! conn (current-thread))
     (thread-send (redis-connection-pool-manager-thread pool) 'new)
     conn)))

(define (connection-pool-return conn [pool (current-redis-pool)])
  (unless pool (redis-error "pool-return: no pool given"))
  (check-owner-ok conn "RETURN")
  ;; this check prevents double returns by the same thread;
  ;; shouldn't get race condition between two different threads since they
  ;; cant own/return the same connection
  (when (hash-has-key? (redis-connection-pool-key=>conn pool) (current-thread))
    (hash-remove! (redis-connection-pool-key=>conn pool) (current-thread))
    (set-redis-connection-owner! conn #f)
    (thread-send (redis-connection-pool-manager-thread pool)
                 (cons (current-thread) conn))))

;; pubsub-specific connections ------------------------------------------------
;; A pubsub connection spins up a few threads:
;; - one handles subscribe/unsubscribe msgs                                   
;;   - created during lease; only created once                                
;; - other threads check for the published messages
;;   - created during make-subscribe-chan
;;   - one per key/async-chan
;; threads are managed by a custodian

(define (pubsub-connection? conn) (redis-connection-pubsub? conn))

(define (lease-pubsub-conn [pool (current-redis-pool)])
  (unless pool (redis-error "pool-lease-pubsub: no pool given"))
  (when (redis-connection-pool-dead? pool)
    (redis-error "Attempted to lease connection from dead connection pool."))
  (define conn (get-idle-or-new-conn pool))
  (define cust (make-custodian))
  (define pubsub-th ; thread to check for sub/unsub confirmations
    (parameterize ([current-custodian cust])
      (thread (lambda ()
        (define in (redis-connection-in conn))
        (let loop ([peek-in (peeking-input-port in)])
          (define reply (get-reply/port peek-in))
          (cond
           [(bytes=? (car reply) #"subscribe")
            (printf "SUBSCRIBE (#~a) ~a confirmed.\n" 
              (caddr reply) (cadr reply))
            ;; commmit the peek
            (unless (port-commit-peeked (file-position peek-in)
                      (port-progress-evt in)
                      always-evt
                      in)
              (redis-error "Could not read subscribe reply"))
            (loop (peeking-input-port in))]
           [(bytes=? (car reply) #"psubscribe")
            (printf "PSUBSCRIBE (#~a) ~a confirmed.\n" 
              (caddr reply) (cadr reply))
            ;; commmit the peek
            (unless (port-commit-peeked (file-position peek-in)
                      (port-progress-evt in)
                      always-evt
                      in)
              (redis-error "Could not read psubscribe reply"))
            (loop (peeking-input-port in))]
           [(bytes=? (car reply) #"unsubscribe")
            (printf "UNSUBSCRIBE ~a confirmed, ~a subscriptions remaining.\n" 
              (cadr reply) (caddr reply))
            ;; commit the peek
            (unless (port-commit-peeked (file-position peek-in)
                      (port-progress-evt in)
                      always-evt
                      in)
              (redis-error "Could not read unsubscribe reply"))
            (loop (peeking-input-port in))]
           [(bytes=? (car reply) #"punsubscribe")
            (printf "PUNSUBSCRIBE ~a confirmed, ~a subscriptions remaining.\n" 
              (cadr reply) (caddr reply))
            ;; commit the peek
            (unless (port-commit-peeked (file-position peek-in)
                      (port-progress-evt in)
                      always-evt
                      in)
              (redis-error "Could not read unsubscribe reply"))
            (loop (peeking-input-port in))]
           [else
            (sync
              (handle-evt (port-progress-evt in)
                (lambda _ (unless (port-closed? in) ; if closed, let thread die
                       (loop (peeking-input-port in))))))]))))))
  (hash-set! (redis-connection-pool-pubsubs pool) conn cust)
  (set-redis-connection-pubsub?! conn #t)
  ;; should owner be #f so connection can be shared between threads?
  (set-redis-connection-owner! conn (current-thread))
  (thread-send (redis-connection-pool-manager-thread pool) 'new)
  conn)

;; make-subscribe-chan : 
;; - (p)subscribes to a key on a pubsub connection
;; - returns an async-channel that listens for that key's messages
(define (make-subscribe-chan conn key 
                             [pool (current-redis-pool)] 
                             #:psubscribe [psub? #f])
  (unless pool (redis-error "pool-make-subscribe-chan: no pool given"))
  (match-define (redis-connection in _ _ pubsub?) conn)
  (unless pubsub? (redis-error "Tried to SUBSCRIBE on non pubsub connection."))
  (send-cmd/no-reply #:rconn conn (if psub? 'psubscribe 'subscribe) key)
  (define ch (make-async-channel)) ; custodians don't affect async chans
  (define pubsubs (redis-connection-pool-pubsubs pool))
  (unless (hash-has-key? pubsubs conn)
    (redis-error 
        "make-subscribe-chan: Not given a known pubsub connection."))
  (define str-key (or (and (string? key) key)
                      (and (symbol? key) (symbol->string key))))
  (parameterize ([current-custodian (hash-ref pubsubs conn)])
    (thread (λ ()
      (let loop ([peek-in (peeking-input-port in)])
        (define reply (get-reply/port peek-in))
        (cond 
         ;; subscribe message
         [(and (bytes=? (car reply) #"message")
               (string=? (bytes->string/utf-8 (cadr reply)) str-key))
          (async-channel-put ch (caddr reply))
          ;; commit the peek               
          (unless (port-commit-peeked (file-position peek-in)
                    (port-progress-evt in)
                    always-evt
                    in)
            (redis-error "Could not read published message."))
          (loop (peeking-input-port in))]
         ;; psubscribe message
         [(and (bytes=? (car reply) #"pmessage")
               (string=? (bytes->string/utf-8 (cadr reply)) str-key))
          (async-channel-put ch (cddr reply))
          ;; commit the peek               
          (unless (port-commit-peeked (file-position peek-in)
                    (port-progress-evt in)
                    always-evt
                    in)
            (redis-error "Could not read published message."))
          (loop (peeking-input-port in))]
         ;; else wait for port progress
         [else
          (sync
            (handle-evt (port-progress-evt in)
              (λ _ (unless (port-closed? in) ; if closed, let thread die   
                     (loop (peeking-input-port in))))))])))))
  ch)

;; returns a pubsub connection to the pool
(define (return-pubsub-conn conn [pool (current-redis-pool)])
  (unless pool (redis-error "pool-return-pubsub: no pool given"))
  (check-owner-ok conn "UNSUBSCRIBE")
  (unless (pubsub-connection? conn)
    (redis-error "Tried to unsubscribe from a non-pubsub connection."))
  (define pubsubs (redis-connection-pool-pubsubs pool))
  ;; this check prevents double close
  (when (hash-has-key? pubsubs conn)
    ;; kill the threads monitoring this connection
    (custodian-shutdown-all (hash-ref pubsubs conn))
    (hash-remove! (redis-connection-pool-pubsubs pool) conn)
    (set-redis-connection-owner! conn #f)
    (set-redis-connection-pubsub?! conn #f)
    (thread-send (redis-connection-pool-manager-thread pool)
                 (cons (current-thread) conn))))


;; send cmd/recv reply --------------------------------------------------------

;; send-cmd/no-reply : sends specified cmd; does not wait for reply
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
  (match-define (redis-connection in out owner _) rconn)
  (check-owner-ok rconn (format "SEND-CMD (~a: ~a) on" cmd args))
  (write-bytes (mk-request cmd args) out)
  (flush-output out)
  rconn)

;; send-cmd : sends given cmd and waits for reply
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
    (get-reply rconn)))

;; get-reply : reads the reply from a redis-connection
;; cmd and args are used for error reporting
;; default in port is the connection for the current thread
(define (get-reply [conn (connection-pool-lease (current-redis-pool))])
  (get-reply/port (redis-connection-in conn)))
(define (get-reply/port [in (redis-connection-in 
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
               (for/list ([n (in-range numreplies)]) (get-reply/port in))))]))

(define (get-reply-evt [conn (connection-pool-lease (current-redis-pool))])
  (get-reply-evt/port (redis-connection-in conn)))
  
(define (get-reply-evt/port 
            [in (redis-connection-in 
                  (connection-pool-lease (current-redis-pool)))])
  (wrap-evt in get-reply/port))

(define (arg->bytes val)
  (define bs
    (cond [(bytes? val) val]
          [(string? val) (string->bytes/utf-8 val)]
          [(number? val) (number->bytes val)]
          [(symbol? val) (symbol->bytes val)]
          [else (error 'send "invalid argument: ~v\n" val)]))
  (bytes-append #"$" (number->bytes (bytes-length bs)) CRLF bs CRLF))

(define (mk-request cmd args)
  (bytes-append
   (string->bytes/utf-8
    (string-append "*" (number->string (add1 (length args))) "\r\n"))
   (arg->bytes cmd)
   (apply bytes-append (map arg->bytes args))))

