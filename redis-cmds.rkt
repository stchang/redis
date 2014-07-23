#lang racket/base
(require "redis.rkt" "bytes-utils.rkt" "constants.rkt")
(require (for-syntax syntax/parse racket/base))
(require data/heap racket/async-channel racket/port racket/set racket/match)

; functions for specific redis commands

(provide (all-defined-out))

;; TODO
;; [x] 2013-09-25: SUBSCRIBE cmd should return (async?) channel?
;;                 DONE: 2014-07-23
;; [o] 2013-09-23: define macro that defines defcmd and defcmds at same time
;; [x] 2013-09-23: define macro that defines all GET/SET variants
;;                 DONE: 2014-07-18

(define-syntax (defcmd stx)
  (syntax-parse stx
    [(_ CMD (~or (~optional (~seq #:return fn) #:defaults ([fn #'(lambda (x) x)]))
                 (~optional (~seq #:no-reply nr) #:defaults ([nr #f]))) ...)
    #:with a-send-cmd (if (attribute nr) #'send-cmd/no-reply #'send-cmd)
    #'(define (CMD #:rconn [conn #f] 
                   #:host [host LOCALHOST]
                   #:port [port DEFAULT-REDIS-PORT] . args)
        (fn 
         (apply a-send-cmd #:rconn conn #:host host #:port port 'CMD args)))]))
(define-syntax-rule (defcmds c ...) (begin (defcmd c) ...))
(define-syntax-rule (defcmd/nr c) (defcmd c #:no-reply #t))
(define-syntax-rule (defcmds/nr c ...) (begin (defcmd/nr c) ...))
(define-syntax-rule (defcmd/chknil c)
  (defcmd c #:return (lambda (reply) (and (not (eq? #\null reply)) reply))))
(define-syntax-rule (defcmds/chknil c ...) (begin (defcmd/chknil c) ...))
(define-syntax-rule (defcmd/ok c)
  (defcmd c #:return (lambda (re) (or (string=? "OK" re) (string=? "QUEUED" re)))))
(define-syntax-rule (defcmds/ok c ...) (begin (defcmd/ok c) ...))
(define-syntax-rule (defcmd/01 c)
  (defcmd c #:return (lambda(reply) (not (and (number? reply) (zero? reply))))))
(define-syntax-rule (defcmds/01 c ...) (begin (defcmd/01 c) ...))

;; basic operations, ie get and set
(defcmds APPEND DEL GETSET MGET SETRANGE STRLEN)
(defcmds/01 EXISTS MSETNX RENAMENX)
(defcmds/chknil GET GETRANGE RANDOMKEY)
(defcmd SET #:return (lambda (reply) (and (not (eq? #\null reply)))))
(defcmds/ok MSET RENAME)
;; SETNX, SETEX, PSETEX: deprecated? use SET + options instead

;; macro to define extension/variation functions of a CMD
;; so you dont have to repeatedly type in all the conn/host/port args
;; WARNING: the following forms capture these variable names:
;; - conn, host port, key, val
(define-syntax (define-CMD-fn stx)
  (syntax-parse stx
    [(_ (CMDfn arg ...) body ...)
     #:with conn (datum->syntax #'CMDfn 'conn)
     #:with host (datum->syntax #'CMDfn 'host)
     #:with port (datum->syntax #'CMDfn 'port)
     #'(define (CMDfn #:rconn [conn #f]
                      #:host [host LOCALHOST]
                      #:port [port DEFAULT-REDIS-PORT] arg ...)
         body ...)]))
;; macro to define variations of the GET CMD
(define-syntax (define-GET stx)
  (syntax-parse stx
    [(_ (GETfn arg ...) body ...)
     #:with key (datum->syntax #'GETfn 'key)
     #'(define-CMD-fn (GETfn key arg ...) body ...)]))
;; macro to define variations of the SET CMD
(define-syntax (define-SET stx)
  (syntax-parse stx
    [(_ (SETfn arg ...) body ...)
     #:with key (datum->syntax #'SETfn 'key)
     #:with val (datum->syntax #'SETfn 'val)
     #'(define-CMD-fn (SETfn key val arg ...) body ...)]))
;; macro to apply a CMD, without having to type all the args
(define-syntax (apply-CMD stx)
  (syntax-parse stx
    [(_ CMDfn arg ...)
     #:with conn (datum->syntax #'CMDfn 'conn)
     #:with host (datum->syntax #'CMDfn 'host)
     #:with port (datum->syntax #'CMDfn 'port)
     #'(CMDfn #:rconn conn #:host host #:port port arg ...)]))
;; macro to apply GET cmd, without having to type all the args
(define-syntax (apply-GET stx)
  (syntax-parse stx
    [(_ GETfn arg ...)
     #:with key (datum->syntax #'GETfn 'key)
     #'(apply-CMD GETfn key arg ...)]))
;; macro to apply SET cmd variation, without having to type all the args
(define-syntax (apply-SET stx)
  (syntax-parse stx
    [(_ SETfn arg ...)
     #:with key (datum->syntax #'SETfn 'key)
     #:with val (datum->syntax #'SETfn 'val)
     #'(apply-CMD SETfn key val arg ...)]))

;; converts bytestring from GET according to conv function
(define-GET (GET/as #:conv [conv (lambda (x) x)])
  (define reply (apply-GET GET))
  (and reply (conv reply)))
;; returns value of key as string (errors if not valid string)
(define-GET (GET/str) (apply-GET GET/as #:conv bytes->string/utf-8))
(define-GET (GET/num) (apply-GET GET/as #:conv bytes->number))
(define-GET (GETRANGE/str start end)
  (define reply (apply-GET GETRANGE start end))
  (and reply (bytes->string/utf-8 reply)))
(define-SET (SET/list)
  (apply-CMD DEL key)
  (for/last ([x val]) (apply-CMD RPUSH key x)))
(define-GET (POP/list #:map-fn [f (lambda (x) x)])
  (let loop ([x (apply-GET LPOP)])
    (if x (cons (f x) (loop (apply-GET LPOP))) null)))
(define-GET (GET/list #:map-fn [f (lambda (x) x)])
  (let loop ([n (sub1 (apply-GET LLEN))] [lst null])
    (if (< n 0) lst
        (let ([x (apply-GET LINDEX n)])
          (if x (loop (sub1 n) (cons (f x) lst)) (loop (sub1 n) lst))))))
(define-GET (GET/set #:map-fn [f (lambda (x) x)])
  (list->set (map f (apply-GET SMEMBERS))))
(define-SET (SET/set)
  (DEL #:rconn conn key)
  (for ([x (in-set val)]) (apply-CMD SADD key x)))
(define-GET (GET/hash #:map-key [fkey (lambda (x) x)] #:map-val [fval (lambda (x) x)])
  (let loop ([lst (apply-GET HGETALL)] [h (hash)])
    (if (null? lst) h 
        (loop (cddr lst) (hash-set h (fkey (car lst)) (fval (cadr lst)))))))
(define-SET (SET/hash)
  (do-MULTI
    (DEL key)
    (for ([(k v) (in-hash val)]) (apply-CMD HSET key k v))))
(define-SET (SET/heap)
  (for ([(k v) (in-hash val)]) (apply-CMD ZADD key v k)))
(define-GET (GET/heap #:map-fn [f (lambda (x) x)] #:map-score [fsco (lambda (x) x)])
  (define hp (make-heap (λ (x y) (<= (car x) (car y)))))
  (let loop ([lst (apply-GET ZRANGE 0 -1 'WITHSCORES)])
    (unless (null? lst)
      (heap-add! hp (cons (fsco (cadr lst)) (f (car lst))))
      (loop (cddr lst))))
  hp)
    

;; DUMP and RESTORE
(defcmd/chknil DUMP)
;; if key to restore exists: (error) ERR Target key name is busy.
(defcmd RESTORE)

;; GETBIT,SETBIT,BITCOUNT,BITOP
;; val = 0 or 1
(defcmds SETBIT GETBIT BITCOUNT)

;; BITOP
(define-syntax-rule (defcmd/bitop OP)
  (define (OP #:rconn [conn #f]
              #:host [host LOCALHOST]
              #:port [port DEFAULT-REDIS-PORT] dest . keys)
    (apply send-cmd #:rconn conn #:host host #:port port 
           "BITOP" 'OP dest keys)))
(define-syntax-rule (defcmds/bitop op ...) (begin (defcmd/bitop op) ...))

(defcmds/bitop AND OR XOR NOT)

(defcmds LPUSH LPUSHX LRANGE LLEN LREM
         RPUSH RPUSHX)
(defcmds/chknil LINDEX LPOP RPOP RPOPLPUSH BLPOP BRPOP BRPOPLPUSH)
(defcmd LINSERT #:return (lambda (reply) (and (not (= -1 reply)) reply)))
(defcmds/ok LSET LTRIM)

;; hashes
(defcmds HDEL HGETALL HINCRBY HINCRBYFLOAT HMGET HVALS)
(defcmd/ok HMSET)
(defcmds/01 HSET HEXISTS HSETNX)
(defcmds/chknil HGET)
(define-GET (HGET/str field)
  (define reply (apply-GET HGET field))
  (and reply (bytes->string/utf-8 reply)))

;; sets
(defcmds SADD SCARD SDIFF SDIFFSTORE SINTER SINTERSTORE SMEMBERS SREM
         SUNION SUNIONSTORE)
(defcmds/01 SISMEMBER SMOVE)
(defcmds/chknil SPOP SRANDMEMBER)

;; ordered sets
(defcmds ZADD ZCARD ZCOUNT ZINCRBY ZINTERSTORE ZRANGE ZRANGEBYSCORE
         ZREM ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE
         ZUNIONSTORE)
(defcmds/chknil ZRANK ZREVRANK ZSCORE)

;; in/decrementing
(defcmds DECR DECRBY INCR INCRBY INCRBYFLOAT HKEYS HLEN)

;; expiring
(defcmds/01 EXPIRE EXPIREAT PERSIST PEXPIRE PEXPIREAT)
(defcmd TTL #:return (lambda (res) (and (not (negative? res)) res)))
(defcmd PTTL #:return (lambda (res) (and (not (negative? res)) res)))

;; MULTI cmds
(defcmds/ok MULTI DISCARD WATCH UNWATCH)
(defcmds/chknil EXEC)
(define-syntax-rule (do-MULTI c ...) (begin (MULTI) c ... (EXEC)))
;; DISCARD EXEC MULTI WATCH UNWATCH

;; pubsub
; (defcmd PUBSUB) ; only available in redis >= v2.8
(defcmds/nr SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE)
(defcmd PUBLISH)

;; administrative commands
;; BGREWRITEAOF BGSAVE INFO LASTSAVE MIGRATE MONITOR MOVE SAVE SHUTDOWN
;; SLAVEOF SLOWLOG SYNC
;; CLIENT: KILL LIST GETNAME SETNAME
;; CONFIG: GET REWRITE SET RESETSTAT
;; DEBUG: OBJECT SEGFAULT

(defcmds DBSIZE ECHO FLUSHALL FLUSHDB KEYS PING TIME TYPE)
(defcmds/ok AUTH QUIT SELECT)

;; LUA scripting
;; EVAL EVALSHA
;; SCRIPT: EXISTS FLUSH KILL LOAD
