#lang racket
(require "redis.rkt"
         "bytes-utils.rkt")
(require (for-syntax syntax/parse))
(require data/heap)

; functions for specific redis commands

(provide (all-defined-out))

;; TODO
;; [o] 2013-09-25: SUBSCRIBE cmd should return (async?) channel?
;; [o] 2013-09-23: define macro that defines defcmd and defcmds at same time
;; [o] 2013-09-23: define macro that defines all GET/SET variants

(define-syntax (defcmd stx)
  (syntax-parse stx
    [(_ CMD (~optional (~seq #:return fn)))
     #:with res #'(apply send-cmd #:rconn rconn 'CMD args) 
     #`(define (CMD #:rconn [rconn (current-redis-connection)] . args)
         #,(if (attribute fn) #'(fn res) #'res))]))
(define-syntax-rule (defcmds c ...) (begin (defcmd c) ...))
(define-syntax-rule (defcmd/chknil c)
  (defcmd c #:return (lambda (reply) (and (not (eq? #\null reply)) reply))))
(define-syntax-rule (defcmds/chknil c ...) (begin (defcmd/chknil c) ...))
(define-syntax-rule (defcmd/ok c)
  (defcmd c #:return (lambda (reply) (string=? "OK" reply))))
(define-syntax-rule (defcmds/ok c ...) (begin (defcmd/ok c) ...))
(define-syntax-rule (defcmd/01 c) (defcmd c #:return (o not zero?)))
(define-syntax-rule (defcmds/01 c ...) (begin (defcmd/01 c) ...))

;; basic operations, ie get and set
(defcmds APPEND DEL GETSET MGET SETRANGE STRLEN)
(defcmds/01 EXISTS MSETNX RENAMENX)
(defcmds/chknil GET GETRANGE RANDOMKEY)
(defcmd SET #:return (lambda (reply) (and (not (eq? #\null reply)))))
(defcmds/ok MSET RENAME)
;; SETNX, SETEX, PSETEX: deprecated? use SET + options instead

;; converts bytestring from GET according to conv function
(define (GET/as #:rconn [rconn (current-redis-connection)] key
                #:conv [conv identity])
  (define reply (GET #:rconn rconn key))
  (and reply (conv reply)))
;; returns value of key as string (errors if not valid string)
(define (GET/str #:rconn [rconn (current-redis-connection)] key)
  (GET/as #:rconn rconn key #:conv bytes->string/utf-8))
(define (GET/num #:rconn [rconn (current-redis-connection)] key)
  (GET/as #:rconn rconn key #:conv bytes->number))
(define (GETRANGE/str #:rconn [rconn (current-redis-connection)] key start end)
  (define reply (GETRANGE #:rconn rconn key start end))
  (and reply (bytes->string/utf-8 reply)))
(define (SET/list #:rconn [rconn (current-redis-connection)] key lst)
  (DEL #:rconn rconn key)
  (for/last ([x lst]) (RPUSH #:rconn rconn key x)))
(define (POP/list #:rconn [rconn (current-redis-connection)] key 
                  #:map-fn [f identity])
  (let loop ([x (LPOP #:rconn rconn key)])
    (if x (cons (f x) (loop (LPOP #:rconn rconn key))) null)))
(define (GET/list #:rconn [rconn (current-redis-connection)] key 
                  #:map-fn [f identity])
  (let loop ([n (sub1 (LLEN #:rconn rconn key))] [lst null])
    (if (< n 0) lst
        (let ([x (LINDEX key n)])
          (if x (loop (sub1 n) (cons (f x) lst)) (loop (sub1 n) lst))))))
(define (GET/set #:rconn [rconn (current-redis-connection)] key 
                  #:map-fn [f identity])
  (list->set (map f (SMEMBERS #:rconn rconn key))))
(define (SET/set #:rconn [rconn (current-redis-connection)] k xs)
  (DEL #:rconn rconn k)
  (for ([x (in-set xs)]) (SADD #:rconn rconn k x)))
(define (GET/hash #:rconn [rconn (current-redis-connection)] key
                  #:map-key [fkey identity] #:map-val [fval identity])
  (let loop ([lst (HGETALL #:rconn rconn key)] [h (hash)])
    (if (null? lst) h 
        (loop (cddr lst) (hash-set h (fkey (car lst)) (fval (cadr lst)))))))
(define (SET/hash #:rconn [rconn (current-redis-connection)] key h)
  (DEL #:rconn rconn key)
  (for ([(k v) (in-hash h)]) (HSET #:rconn rconn key k v)))
(define (SET/heap #:rconn [rconn (current-redis-connection)] key h)
  (for ([(k v) (in-hash h)]) (ZADD #:rconn rconn key v k)))
(define (GET/heap #:rconn [rconn (current-redis-connection)] key
                  #:map-fn [f identity] #:map-score [fsco identity])
  (define hp (make-heap (λ (x y) (<= (car x) (car y)))))
  (let loop ([lst (ZRANGE key 0 -1 'WITHSCORES)])
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
  (define (OP #:rconn [rconn (current-redis-connection)] dest . keys)
    (apply send-cmd #:rconn rconn "BITOP" 'OP dest keys)))
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
(define (HGET/str #:rconn [rconn (current-redis-connection)] key field)
  (define reply (HGET #:rconn rconn key field))
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
(defcmds PUBLISH SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE)

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