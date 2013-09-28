#lang racket
(require "redis.rkt")
(require (for-syntax syntax/parse))

; functions for specific redis commands

(provide (all-defined-out))

;; TODO
;; [o] 2013-09-25: SUBSCRIBE cmd should return (async?) channel?
;; [o] 2013-09-23: define macro that defines defcmd and defcmds at same time
;; [o] 2013-09-23: define macro that defines all GET/SET variants

(define o compose)

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
;; returns value of key as string (errors if not valid string)
(define (GET/str #:rconn [rconn (current-redis-connection)] key)
  (define reply (GET #:rconn rconn key))
  (and reply (bytes->string/utf-8 reply)))
(define (GETRANGE/str #:rconn [rconn (current-redis-connection)] key start end)
  (define reply (GETRANGE #:rconn rconn key start end))
  (and reply (bytes->string/utf-8 reply)))
(define (GET/num #:rconn [rconn (current-redis-connection)] key)
  (define reply (GET #:rconn rconn key))
  (and reply (string->number (bytes->string/utf-8 reply))))
(define (SET/list #:rconn [rconn (current-redis-connection)] key lst)
  (DEL #:rconn rconn key)
  (for/last ([x lst]) (RPUSH #:rconn rconn key x)))
(define (GET/list #:rconn [rconn (current-redis-connection)] key)
  (let loop ([x (LPOP #:rconn rconn key)])
    (if x (cons x (loop (LPOP #:rconn rconn key))) null)))

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
;; BGREWRITEAOF BGSAVE DBSIZE INFO LASTSAVE MIGRATE MONITOR MOVE SAVE SHUTDOWN
;; SLAVEOF SLOWLOG SYNC
;; CLIENT: KILL LIST GETNAME SETNAME
;; CONFIG: GET REWRITE SET RESETSTAT
;; DEBUG: OBJECT SEGFAULT

(defcmds ECHO FLUSHALL FLUSHDB KEYS PING TIME TYPE)
(defcmds/ok AUTH QUIT SELECT)

;; LUA scripting
;; EVAL EVALSHA
;; SCRIPT: EXISTS FLUSH KILL LOAD