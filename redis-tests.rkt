#lang at-exp racket
(require "redis.rkt" "redis-cmds.rkt"
         "bytes-utils.rkt" "test-utils.rkt")
(require rackunit data/heap racket/async-channel)

(module+ main (displayln "Run tests with \"raco test redis-tests.rkt\"."))

(module+ test
  
  (printf 
    @~a{
        
        NOTE: These tests assume a locally running redis server on port 6379.
        
        *WARNING*: Interrupting these tests will result in data loss.
              
        * These tests temporarily delete keys from the db. At the conclusion of
        the tests, all keys are restored to their original values, but if the 
        tests are interrupted, the restore won't occur and you'll lose data.
        
        * Thus, prior to running the tests, we recommend restarting the server
        with keys loaded from the included "test.rdb".
        
        * As a precaution, before the tests start:
        - all keys are saved to the current rdb file (as pointed to by the 
          "dir" and "dbfilename" config parameters),
        - the current rdb file is backed up to this directory (with an
          additional ".bak" extension.),
        - the "dir" and "dbfilename" config parameters are saved and changed
          to temporary values, so intermediate db states are not accidentally
          written to disk.
        
        * At the conclusion of the tests, in additional to the original key 
        values, the "dir" and "dbfilename" parameters are also restored.

        * If the tests are interrupted, you'll have to manually restore the 
        config parameters, as well as the keys in the db.

        Continue with the tests? (enter 'y' to continue): })
  
  ;; abort unless "y" is entered
  (unless (eq? 'y (read)) (printf "tests aborted\n") (exit))
  
  ;; "y" entered, so continue with tests
  
  ;; save keys to disk
  (printf "\nSaving keys to disk ..........\n")
  (send-cmd 'save)
  
  ;; save current "dir" and "dbfilename" config params
  (define rdb-dir
    (bytes->string/utf-8 (second (send-cmd 'config 'get 'dir))))
  (define rdb-file
    (bytes->string/utf-8 (second (send-cmd 'config 'get 'dbfilename))))
  (printf "\nSaving current parameters: ..........\n")
  (printf "current \"dir\": ~a\n" rdb-dir)
  (printf "current \"dbfilename\": ~a\n" rdb-file)
  ;; eg, /var/lib/redis/6379/dump.rdb
  (define rdb-path (build-path rdb-dir rdb-file))

  ;; save current rdb file to this test directory
  (define test-dir (path->string (current-directory)))
  (define tmp-rdb-file "tmp.rdb")
  (define backup-path (build-path test-dir (~a rdb-file ".bak")))
  (printf "\nBacking up rdb file: ..........\n- from: ~a\n- to: ~a\n"
    rdb-path backup-path)
  (copy-file rdb-path backup-path #t) ; back up current rdb file
                     
  ;; set temporary "dir" and "dbfilename" params
  (printf "\nSetting tmp dir: ~a\n" test-dir)
  (send-cmd 'config 'set 'dir test-dir)
  (printf "Setting tmp rdb file: ~a\n" tmp-rdb-file)
  (send-cmd 'config 'set 'dbfilename tmp-rdb-file)
  
  ;; do tests  
  (printf "\nRunning tests ..........\n")

  ;; BEGIN TESTS --------------------------------------------------------------
  
;; basic connection
(define conn1 (connect))
(check-true (SET #:rconn conn1 "x" 100)) 
(check-equal? (GET/str #:rconn conn1 "x") "100")
(check-not-exn (lambda () (disconnect conn1)))

; test GET, SET, EXISTS, DEL, APPEND, etc
(test
  (check-true (SET "x" 100))
  (check-equal? (GET/str "x") "100")
  (check-true (EXISTS "x"))
  (check-equal? (APPEND "x" "World") 8)
  (check-equal? (GET/str "x") "100World")
  (check-true (SET "y" "hello"))
  (check-equal? (GET/str "y") "hello")
  (check-equal? (GETSET "y" "world") #"hello")
  (check-equal? (GET/str "y") "world")
  (check-equal? (MGET "x" "y") (list #"100World" #"world"))
  (check-true (SET "z" "This is a string"))
  (check-equal? (GETRANGE "z" 0 3) #"This")
  (check-equal? (GETRANGE "z" -3 -1) #"ing")
  (check-equal? (GETRANGE "z" 0 -1) #"This is a string")
  (check-equal? (GETRANGE/str "z" 10 100) "string")
  (check-equal? (DEL "x" "y" "z") 3)
  (check-true (MSETNX "k1" "Hello" "k2" "there"))
  (check-false (MSETNX "k2" "there" "k3" "world"))
  (check-equal? (MGET "k1" "k2" "k3") (list #"Hello" #"there" #\null))
  ;; setrange
  (check-true (SET 'key1 "Hello World"))
  (check-equal? (SETRANGE 'key1 6 "Redis") 11)
  (check-equal? (GET/str 'key1) "Hello Redis")
  (check-equal? (SETRANGE 'key2 6 "Redis") 11)
  (check-equal? (GET 'key2) #"\0\0\0\0\0\0Redis"))

; test SET options
(test
  (check-true (SET "z" "red" "EX" 1)) ; expire in 1 second
  (check-equal? (GET/str "z") "red")
  (sleep 1.5)
  (check-false (GET/str "z"))
  (check-equal? (do-MULTI (SET "z" "blue" "PX" 1) ; expire in 1 ms
                          (GET "z") "blue")
                (list "OK" #"blue"))
  (sleep .1)
  (check-false (GET/str "z"))
  (check-false (EXISTS "z"))
  (check-false (SET "z" "black" "XX"))
  (check-false (EXISTS "z"))
  (check-true (SET "z" "yellow" "NX"))
  (check-equal? (GET/str "z") "yellow")
  (check-true (SET "z" "white" "XX"))
  (check-equal? (GET/str "z") "white"))

;; test SETBIT,GETBIT, BITCOUNT
(test
  (check-true (SET "pin" "foobar"))
  (check-equal? (BITCOUNT "pin") 26)
  (check-equal? (BITCOUNT "pin" 0 0) 4)
  (check-equal? (BITCOUNT "pin" 1 1) 6)
  (check-equal? (SETBIT "pan" 7 1) 0)
  (check-equal? (BITCOUNT "pan") 1)
  (check-equal? (SETBIT "pan" 7 0) 1)
  (check-equal? (GETBIT "pan" 7) 0)
  (check-equal? (GET "pan") #"\0")
  (check-equal? (SETBIT "pon" 4 1) 0)
  (check-equal? (GET "pon") #"\b"))

;; test BITOP
(test
  (check-true (SET "hip" "foobar"))
  (check-true (SET "hop" "abcdef"))
  (check-equal? (AND "hep" "hip" "hop") 6)
  (check-equal? (GET/str "hep") "`bc`ab")
  (check-equal? (DEL "hip" "hop") 2)
  (check-equal? (SETBIT "hip" 0 1) 0)
  (check-equal? (SETBIT "hop" 0 0) 0)
  (check-equal? (AND "hep" "hip" "hop") 1)
  (check-equal? (GETBIT "hep" 0) 0)
  (check-equal? (OR "hep" "hip" "hop") 1)
  (check-equal? (GETBIT "hep" 0) 1)
  (check-equal? (NOT "hep" "hop") 1)
  (check-equal? (GETBIT "hep" 0) 1)
  (check-equal? (XOR "hep" "hip" "hop") 1)
  (check-equal? (GETBIT "hep" 0) 1)
  (check-equal? (SETBIT "hop" 0 1) 0)
  (check-equal? (XOR "hep" "hip" "hop") 1)
  (check-equal? (GETBIT "hep" 0) 0))

;; test list commands
(test
  (check-equal? (LPUSH "lst" "world") 1)
  (check-equal? (LPUSHX "lst" "hello") 2)
  (check-equal? (LRANGE "lst" 0 -1) (list #"hello" #"world"))
  (check-equal? (LPUSHX "lst-non-exist" "Hello") 0)
  (check-equal? (LRANGE "lst-non-exist" 0 -1) null)

  (check-equal? (DEL "lst") 1)

  ;; LINDEX test
  (check-equal? (LPUSH "lst" "World") 1)
  (check-equal? (LPUSH "lst" "Hello") 2)
  (check-equal? (LINDEX "lst" 0) #"Hello")
  (check-equal? (LINDEX "lst" -1) #"World")
  (check-false (LINDEX "lst" 3))
  
  (check-equal? (DEL "lst") 1)

  ;; LINSERT test
  (check-equal? (RPUSH "lst" "Hello") 1)
  (check-equal? (RPUSHX "lst" "World") 2)
  (check-equal? (RPUSHX "lst-non-exist" "asdf") 0)
  (check-false (LINSERT "lst" 'BEFORE "Worl" "There"))
  (check-equal? (LINSERT "lst" 'BEFORE "World" "There") 3)
  (check-equal? (LRANGE "lst" 0 -1) (list #"Hello" #"There" #"World"))
  (check-equal? (LLEN "lst") 3)
  (check-false (RPOPLPUSH "lst-non-exist" "lst2"))
  (check-equal? (RPOPLPUSH "lst" "lst2") #"World")
  (check-equal? (LRANGE "lst" 0 -1) (list #"Hello" #"There"))
  (check-equal? (LRANGE "lst2" 0 -1) (list #"World"))

  (check-equal? (DEL "lst") 1)

  ;; L/RPOP test
  (check-equal? (RPUSH "lst" "one") 1)
  (check-equal? (RPUSH "lst" "two") 2)
  (check-equal? (RPUSH "lst" "three") 3)
  (check-equal? (LPOP "lst") #"one")
  (check-equal? (LRANGE "lst" 0 -1) (list #"two" #"three"))
  (check-equal? (RPOP "lst") #"three")
  (check-equal? (LRANGE "lst" 0 -1) (list #"two"))
  
  (check-equal? (DEL "lst") 1)

  ;; LREM test
  (check-equal? (RPUSH "lst" "hello") 1)
  (check-equal? (RPUSH "lst" "hello") 2)
  (check-equal? (RPUSH "lst" "foo") 3)
  (check-equal? (RPUSH "lst" "hello") 4)
  (check-equal? (LREM "lst" -2 "hello") 2)
  (check-equal? (LRANGE "lst" 0 -1) (list #"hello" #"foo"))

  (check-equal? (DEL "lst") 1)

  ;; LSET test
  (check-equal? (RPUSH "lst" "one") 1)
  (check-equal? (RPUSH "lst" "two") 2)
  (check-equal? (RPUSH "lst" "three") 3)
  (check-true (LSET "lst" 0 "four"))
  (check-true (LSET "lst" -2 "five"))
  (check-equal? (LRANGE "lst" 0 -1) (list #"four" #"five" #"three"))
  (check-redis-exn (LSET "lst" 4 "six"))
  (check-true (LTRIM "lst" 1 -1))
  (check-equal? (LRANGE "lst" 0 -1) (list #"five" #"three"))

  ;; blocking pop
  (check-equal? (DEL "lst" "lst2") 2)

  (check-equal? (RPUSH "lst" 'a 'b 'c) 3)
  (check-equal? (BLPOP "lst" "lst2" 0) (list #"lst" #"a"))
  (check-equal? (BRPOPLPUSH "lst" "lst2" 0) #"c")
  (check-equal? (BRPOP "lst" "lst2" 0) (list #"lst" #"b"))
  (check-equal? (BRPOP "lst" "lst2" 0) (list #"lst2" #"c"))
  (check-false (BLPOP "lst-non-exist" 1))
  (check-false (BRPOP "lst-non-exist" 1))
  (check-false (BRPOPLPUSH "lst-non-exist" "lst" 1))

  (define achan (make-async-channel))
  ;; new thread will automatically get new connection
  (thread (lambda () (async-channel-put achan (BRPOP "lst" 0))))
  (check-equal? (RPUSH "lst" "aaa") 1)
  (check-equal? (async-channel-get achan) (list #"lst" #"aaa"))
  (thread (lambda () (async-channel-put achan (BLPOP "lst" 0))))
  (check-equal? (RPUSH "lst" "bbb") 1)
  (check-equal? (async-channel-get achan) (list #"lst" #"bbb"))
  (thread (lambda () (async-channel-put achan (BRPOPLPUSH "lst" "lst2" 0))))
  (check-equal? (RPUSH "lst" "ccc") 1)
  (check-equal? (async-channel-get achan) #"ccc")
  )

(test
  (check-equal? (SET/list "lst" (list 1 2 3)) 3)
  (check-equal? (GET/list "lst") (list #"1" #"2" #"3"))
  (check-equal? (POP/list "lst") (list #"1" #"2" #"3"))
  (SET/hash "hsh" (hash 'a 10 'b 20 'c 30))
  (check-equal? (GET/hash "hsh" #:map-key bytes->symbol
                                #:map-val bytes->number)
                (hash 'a 10 'b 20 'c 30))
  (SET/set "s" (set 1 2 3 4 5))
  (check-equal? (GET/set "s" #:map-fn bytes->number) (set 1 2 3 4 5))
  (SET/heap "hp" (hash 'a 30 'b 10 'c 20))
  (check-equal? (heap->vector
                 (GET/heap "hp" #:map-fn bytes->symbol
                                #:map-score bytes->number))
                (let ([h (make-heap (lambda (x y) (<= (car x) (car y))))])
                  (heap-add-all! h '((30 . a) (10 . b) (20 . c)))
                  (heap->vector h)))
  )

;; in/decrementing
(test
  (check-true (SET "i" 10))
  (check-equal? (INCR "i") 11)
  (check-equal? (GET/num "i") 11)
  (check-equal? (INCRBY "i" 5) 16)
  (check-equal? (GET/num "i") 16)
  (check-equal? (INCRBYFLOAT "i" 0.5) #"16.5")
  (check-equal? (GET/num "i") 16.5)
  (check-redis-exn (DECR "i")) ;; not int
  (check-equal? (DECR "j") -1)
  (check-equal? (DECRBY "j" 2) -3)
  (check-equal? (GETSET "i" 0) #"16.5")
  (check-equal? (GET/num "i") 0))

;; flush/expire
(test
  (check-true (MSET "x" 10 "y" 11 "z" 12))
  (check-true (EXISTS "x"))
  (check-true (EXISTS "y"))
  (check-true (EXISTS "z"))
  (FLUSHALL)
  (check-false (EXISTS "x"))
  (check-false (EXISTS "y"))
  (check-false (EXISTS "z"))
  (check-true (MSET "x" 10 "y" 11 "z" 12))
  (check-true (EXISTS "x"))
  (check-true (EXISTS "y"))
  (check-true (EXISTS "z"))
  (FLUSHDB)
  (check-false (EXISTS "x"))
  (check-false (EXISTS "y"))
  (check-false (EXISTS "z"))

  ;; expire
  (check-false (EXPIRE "x" 1))
  (check-false (EXPIREAT "x" 1))
  (check-true (MSET "x" 10 "y" 11 "z" 12))
  (check-true (EXPIRE "x" 2))
  (check-equal? (TTL "x") 2)
  (sleep 2.1)
  (check-false (EXISTS "x"))
  (check-true (EXPIRE "y" 1))
  (check-true (PERSIST 'y))
  (sleep 1.1)
  (check-true (EXISTS "y"))
  (check-equal? (GET/num 'y) 11)
  (check-true (PEXPIRE 'y 100))
  (check-true (let ([x (PTTL 'y)]) (or (= x 100) (= x 99) (= x 98))))
  (sleep .11)
  (check-false (EXISTS 'y))
  (check-false (PEXPIREAT 'y 100))
  (check-false (TTL "non-exist")) ; nonexisting, -2
  (check-false (TTL "y"))) ; no TTL, -1

;; hashes
(test
  (check-true (HSET "h" 'field1 "foo")) ; #t means new field
  (check-true (HEXISTS "h" 'field1))
  (check-equal? (HGET "h" 'field1) #"foo")
  (check-equal? (HGET/str "h" 'field1) "foo")
  (check-false (HEXISTS "h" 'field2))
  (check-false (HGET "h" 'field2))
  (check-false (HSET "h" 'field1 "bar")) ; #f means field exists
  (check-equal? (HGET/str "h" 'field1) "bar")
  (check-true (HSET "h" 'field2 "world"))
  (check-equal? (HKEYS "h") (list #"field1" #"field2"))
  (check-equal? (HKEYS "h2") null)
  (check-equal? (HGETALL "h") (list #"field1" #"bar" #"field2" #"world"))
  (check-equal? (HLEN "h") 2)
  (check-equal? (HLEN "h2") 0)
  (check-equal? (HDEL "h" 'field1) 1)
  (check-equal? (HDEL "h" 'field1) 0)
  (check-true (HSET "h" 'num 5))
  (check-equal? (HINCRBY "h" 'num 1) 6)
  (check-equal? (HINCRBY "h" 'num -1) 5)
  (check-equal? (HINCRBY "h" 'num -10) -5)
  (check-equal? (HINCRBYFLOAT "h" 'num .1) #"-4.9")
  (check-false (HSET "h" 'num "5.0e3"))
  (check-equal? (HINCRBYFLOAT "h" 'num "2.0e2") #"5200")
  (check-equal? (HMGET "h" 'field1 'field2 'num)
                (list #\null #"world" #"5200"))
  (check-true (HMSET "h" 'field1 "Hello" 'field2 "World"))
  (check-equal? (HMGET "h" 'field1 'field2) (list #"Hello" #"World"))
  (check-false (HSETNX "h" 'field1 "hall"))
  (check-true (HSET "h" 'field3 "hall"))
  (check-equal? (HGET/str "h" 'field3) "hall")
  (check-equal? (HVALS "h") (list #"World" #"5200" #"Hello" #"hall")))

;; KEYS testing
(test
  (check-true (MSET "one" 1 "two" 2 "three" 3 "four" 4))
  (define keyso (KEYS "*o*"))
  (check-equal? #"one" (car (member #"one" keyso)))
  (check-equal? #"two" (car (member #"two" keyso)))
  (check-equal? #"four" (car (member #"four" keyso)))
  (check-equal? (KEYS "t??") (list #"two"))
  (define keys* (KEYS "*"))
  (check-equal? #"one" (car (member #"one" keys*)))
  (check-equal? #"two" (car (member #"two" keys*)))
  (check-equal? #"three" (car (member #"three" keys*)))
  (check-equal? #"four" (car (member #"four" keys*))))

;; MULTI
(test
  (check-equal? (do-MULTI (INCR "foo") (INCR "bar")) (list 1 1))
  ;; check exn
  (check-redis-exn (do-MULTI (SET "a" 3) (LPOP "a")))
  ;; check discard
  (check-true (SET 'foo 1))
  (check-true (MULTI))
  (check-equal? (INCR 'foo) "QUEUED")
  (check-true (DISCARD))
  (check-equal? (GET/num 'foo) 1)
  ;; WATCH: no abort
  (check-true (WATCH 'foo))
  (check-equal? (do-MULTI (SET 'foo 10)) (list "OK"))
  (check-equal? (GET/num 'foo) 10)
  ;; WATCH: abort
  (check-true (WATCH 'foo))
  (check-equal? (INCR 'foo) 11)
  (check-false (do-MULTI (SET 'foo 10)))
  (check-equal? (GET/num 'foo) 11)
  ;; WATCH/UNWATCH
  (check-true (WATCH 'foo))
  (check-true (UNWATCH))
  (check-equal? (INCR 'foo) 12)
  (check-equal? (do-MULTI (SET 'foo 10)) (list "OK"))
  (check-equal? (GET/num 'foo) 10))

;; pub/sub
(test
  ;; sub/unsub
 (SUBSCRIBE 'foo 'bar)
 (check-equal? (get-reply) (list #"subscribe" #"foo" 1))
 (check-equal? (get-reply) (list #"subscribe" #"bar" 2))
 (PSUBSCRIBE "news.*")
 (check-equal? (get-reply) (list #"psubscribe" #"news.*" 3))
 (thread (lambda () (PUBLISH 'foo "Hello"))) ; publish with new connection
 (check-equal? (get-reply) (list #"message" #"foo" #"Hello"))
 (thread (lambda () (PUBLISH 'foo "Kello"))) ; publish with new connection
 (check-equal? (sync (get-reply-evt)) (list #"message" #"foo" #"Kello"))
 ;; test for the bug fixed by m4burns, where a SUBSCRIBE
 ;; followed by a get-reply can accidentally read a message from another
 ;; subscribe instead of the SUBSCRIBE cmd reply msg
 (sync (thread (lambda () (PUBLISH 'foo "Jello")))) ; wait for thread to finish
 (SUBSCRIBE 'goo)
 (check-equal? (get-reply) (list #"message" #"foo" #"Jello"))
 (check-equal? (get-reply) (list #"subscribe" #"goo" 4))
 ;; test unsubscribe
 (UNSUBSCRIBE 'foo)
 (check-equal? (get-reply) (list #"unsubscribe" #"foo" 3))
 (UNSUBSCRIBE)
 (check-equal? (get-reply) (list #"unsubscribe" #"goo" 2))
 (check-equal? (get-reply) (list #"unsubscribe" #"bar" 1))
 ;; psub/punsub
 ;; (check-equal? (PSUBSCRIBE "news.*") (list #"psubscribe" #"news.*" 1))
 (thread (lambda () (PUBLISH 'news.art "Pello")))
 (check-equal? (get-reply)
   (list #"pmessage" #"news.*" #"news.art" #"Pello"))
 (PUNSUBSCRIBE "news.*")
 (check-equal? (get-reply) (list #"punsubscribe" #"news.*" 0))
 ;; PUBSUB cmd only available in redis version >= 2.8
 )

;; pubsub2: using pubsub-specific connections
(test
 (define conn1 (lease-pubsub-conn))
 (check-true (pubsub-connection? conn1))
 (define foo-ch (make-subscribe-chan conn1 'foo))
 (define bar-ch (make-subscribe-chan conn1 'bar))
 (define news*-ch (make-subscribe-chan conn1 "news.*" #:psubscribe #t))
 (thread (lambda () (PUBLISH 'foo "Hello"))) ; publish with new connection
 (check-equal? (sync/timeout .1 foo-ch) #"Hello")
 (thread (lambda () (PUBLISH 'foo "Kello"))) ; publish with new connection
 (check-equal? (sync/timeout .1 foo-ch) #"Kello")
 ;; test for the bug fixed by m4burns, where a SUBSCRIBE
 ;; followed by a get-reply can accidentally read a message from another
 ;; subscribe instead of the SUBSCRIBE cmd reply msg
 (thread (lambda () (PUBLISH 'foo "Jello"))) ; publish with new connection
 (sleep 0.01)
 (define goo-ch (make-subscribe-chan conn1 'goo))
 (check-equal? (sync/timeout .1 foo-ch) #"Jello")
 ;; test unsubscribe
 (UNSUBSCRIBE #:rconn conn1 'foo)
 (thread (lambda () (PUBLISH 'foo "Mello"))) ; publish with new connection
 (check-false (async-channel-try-get foo-ch)) ; test connection closed
 
 (UNSUBSCRIBE #:rconn conn1)
 (thread (lambda () (PUBLISH 'bar "Mello"))) ; publish with new connection
 (thread (lambda () (PUBLISH 'goo "Mello"))) ; publish with new connection
 (check-false (async-channel-try-get bar-ch))
 (check-false (async-channel-try-get goo-ch))

 ;; news.* still psubscribed to conn1
 (thread (lambda () (PUBLISH 'news.art "Pello")))
 (check-equal? (sync/timeout .1 news*-ch) (list #"news.art" #"Pello"))
 (PUNSUBSCRIBE #:rconn conn1 "news.*")
 (thread (lambda () (PUBLISH 'news.art "Qello")))
 (check-false (async-channel-try-get news*-ch))
)

; random key
(test
  (check-false (RANDOMKEY))
  (SET 'x 100)
  (check-equal? (RANDOMKEY) #"x"))

; rename
(test
  (check-redis-exn (RENAME 'key1 'key2))
  (check-true (SET 'key1 "Hello"))
  (check-redis-exn (RENAME 'key1 'key1))
  (check-true (RENAME 'key1 'key2))
  (check-equal? (GET/str 'key2) "Hello")
  (check-redis-exn (RENAMENX 'key3 'key4))
  (check-true (SET 'key3 "Dello"))
  (check-redis-exn (RENAMENX 'key3 'key3))
  (check-false (RENAMENX 'key3 'key2))
  (check-equal? (GET/str 'key2) "Hello")
  (check-true (RENAMENX 'key3 'key4))
  (check-equal? (GET/str 'key4) "Dello"))

;; strlen
(test
  (check-true (SET 'k "Hello World"))
  (check-equal? (STRLEN 'k) 11)
  (check-equal? (STRLEN 'non-exist) 0)
  (check-equal? (RPUSH 'lst 1) 1)
  (check-redis-exn (STRLEN 'lst)))

;; type
(test
  (check-true (SET 'k:str "val"))
  (check-equal? (TYPE 'k:str) "string")
  (check-equal? (LPUSH 'k:lst "val") 1)
  (check-equal? (TYPE 'k:lst) "list")
  (check-true (HSET 'k:hash 'field1 "val"))
  (check-equal? (TYPE 'k:hash) "hash")
  (check-equal? (SADD 'k:set "val") 1)
  (check-equal? (TYPE 'k:set) "set"))

;; sets
(test
  (check-true (SET 'not-set 1))
  (check-redis-exn (SADD 'not-set 1))
  (check-equal? (SADD 'myset "Hello") 1)
  (check-equal? (SADD 'myset "World") 1)
  (check-equal? (SADD 'myset "World") 0)
  (check-true (SISMEMBER 'myset "Hello"))
  (check-false (SISMEMBER 'myset "Dello"))
  (check-set-equal? (SMEMBERS 'myset) (#"Hello" #"World"))
  (check-equal? (SCARD 'myset) 2)
  (check-equal? (SADD 'myset "H" "E" "L" "L" "O") 4)
  (check-set-equal? (SMEMBERS 'myset)(#"Hello" #"World" #"H" #"E" #"L" #"O"))
  (check-equal? (SCARD 'myset) 6)
  (check-equal? (SADD 's1 #"A") 1)
  (check-equal? (list->set (SDIFF 'myset 's1)) (list->set (SMEMBERS 'myset)))
  (check-equal? (SADD 's2 #"H" #"E") 2)
  (check-set-equal? (SDIFF 'myset 's1 's2) (#"Hello" #"World" #"L" #"O"))
  (check-equal? (SDIFFSTORE 'destset 'myset 's1 's2) 4)
  (check-set-equal? (SMEMBERS 'destset) (#"Hello" #"World" #"L" #"O"))
  (check-set-equal? (SINTER 'myset 's1) ())
  (check-set-equal? (SINTER 'myset 's2) (#"H" #"E"))
  (check-set-equal? (SINTER 'myset 's1 's2) ())
  (check-equal? (SINTERSTORE 'destset 'myset 's2) 2)
  (check-set-equal? (SMEMBERS 'destset) (#"H" #"E"))
  (check-redis-exn (SMOVE 'not-set 'myset 1))
  (check-redis-exn (SMOVE 'myset 'not-set 1))
  (check-false (SMOVE 'myset 's1 "Dello"))
  (check-true (SMOVE 'myset 's1 "Hello"))
  (check-set-equal? (SMEMBERS 'myset) (#"World" #"H" #"E" #"L" #"O"))
  (check-set-equal? (SMEMBERS 's1) (#"Hello" #"A"))
  (check-true (let ([x (SRANDMEMBER 's1)])
                (or (equal? x #"Hello") (equal? x #"A"))))
  (check-set-equal? (SRANDMEMBER 's1 2) (#"Hello" #"A"))
  (check-set-equal? (SRANDMEMBER 's1 3) (#"Hello" #"A"))
  (check-true (let ([xs (list->set (SRANDMEMBER 's1 -3))])
                (or (equal? xs (set #"Hello" #"A"))
                    (equal? xs (set #"A")) (equal? xs (set #"Hello")))))
  (check-true (let ([x (SPOP 's1)])
                (or (equal? x #"Hello") (equal? x #"A"))))
  (check-true (let ([x (SPOP 's1)])
                (or (equal? x #"Hello") (equal? x #"A"))))
  (check-false (SPOP 's1))
  (check-set-equal? (SRANDMEMBER 's1 1) ())
  (check-redis-exn (SRANDMEMBER 'not-set 1))
  (check-set-equal? (SRANDMEMBER 'non-exist 1) ())
  (check-false (SRANDMEMBER 'non-exist))
  (check-false (SPOP 'non-exist))
  (check-equal? (SREM 'myset "World") 1)
  (check-equal? (SREM 'myset "World") 0)
  (check-equal? (SREM 'myset "H" "E" "L") 3)
  (check-set-equal? (SMEMBERS 'myset) (#"O"))
  (check-redis-exn (SREM 'not-set 1))
  (check-equal? (SREM 'non-exist 1) 0)
  (check-set-equal? (SUNION 'myset 's2) (#"H" #"E" #"O"))
  (check-equal? (SUNIONSTORE 'destset 'myset 's2) 3)
  (check-set-equal? (SMEMBERS 'destset) (#"H" #"E" #"O")))

;; sorted sets
(test
  (check-true (SET 'not-zset 1))
  (check-redis-exn (ZADD 'not-zset 1))
  (check-equal? (ZADD 'zset 1 "one") 1)
  (check-equal? (ZCARD 'zset) 1)
  (check-equal? (ZADD 'zset 1 "uno") 1)
  (check-equal? (ZCARD 'zset) 2)
  (check-equal? (ZADD 'zset 2 "two") 1)
  (check-equal? (ZCARD 'zset) 3)
  (check-equal? (ZADD 'zset 3 "two") 0)
  (check-equal? (ZCARD 'zset) 3)
  (check-equal? (ZCARD 'non-exist) 0)
  (check-equal? (ZRANGE 'zset 0 -1 'WITHSCORES)
                (list #"one" #"1" #"uno" #"1" #"two" #"3"))
  (check-equal? (ZRANGE 'zset 0 -1)
                (list #"one" #"uno" #"two"))
  (check-equal? (ZREVRANGE 'zset 0 -1 'WITHSCORES)
                (list #"two" #"3" #"uno" #"1" #"one" #"1"))
  (check-equal? (ZREVRANGE 'zset 0 -1)
                (list #"two" #"uno" #"one"))
  (check-equal? (ZCOUNT 'zset "-inf" "+inf") 3)
  (check-equal? (ZCOUNT 'zset "(1" 3) 1)
  (check-equal? (ZINCRBY 'zset 4 "one") #"5")
  (check-equal? (ZRANGE 'zset 0 -1 'WITHSCORES)
                (list #"uno" #"1" #"two" #"3" #"one" #"5"))
  (check-equal? (ZADD 'zset2 1 "one" 2 "two" 3 "three") 3)
  (check-equal? (ZINTERSTORE 'zdest 2 'zset 'zset2 'WEIGHTS 2 3) 2)
  (check-equal? (ZRANGE 'zdest 0 -1 'WITHSCORES)
                (list #"two" #"12" #"one" #"13"))
  (check-equal? (ZINTERSTORE 'zdest 2 'zset 'zset2 'WEIGHTS 2 3
                                                   'AGGREGATE 'SUM) 2)
  (check-equal? (ZRANGE 'zdest 0 -1 'WITHSCORES)
                (list #"two" #"12" #"one" #"13"))
  (check-equal? (ZINTERSTORE 'zdest 2 'zset 'zset2 'WEIGHTS 2 3
                                                   'AGGREGATE 'MAX) 2)
  (check-equal? (ZRANGE 'zdest 0 -1 'WITHSCORES)
                (list #"two" #"6" #"one" #"10"))
  (check-equal? (ZINTERSTORE 'zdest 2 'zset 'zset2 'WEIGHTS 2 3
                                                   'AGGREGATE 'MIN) 2)
  (check-equal? (ZRANGE 'zdest 0 -1 'WITHSCORES)
                (list #"one" #"3" #"two" #"6"))
  (check-equal? (ZRANGEBYSCORE 'zset "-inf" "+inf")
                (list #"uno" #"two" #"one"))
  (check-equal? (ZRANGEBYSCORE 'zset "-inf" "+inf" 'WITHSCORES)
                (ZRANGE 'zset 0 -1 'WITHSCORES))
  (check-equal? (ZRANGEBYSCORE 'zset "(1" "5") (list #"two" #"one"))
  (check-equal? (ZRANGEBYSCORE 'zset "(1" "5" 'LIMIT 0 1) (list #"two"))
  (check-equal? (ZRANGEBYSCORE 'zset "(1" "5" 'LIMIT 1 1) (list #"one"))
  (check-equal? (ZRANGEBYSCORE 'zset "(1" "5" 'LIMIT 2 1) (list))
  (check-equal? (ZRANGEBYSCORE 'zset "(1" "(5") (list #"two"))
  (check-equal? (ZRANK 'zset "two") 1)
  (check-equal? (ZRANK 'zset "one") 2)
  (check-equal? (ZRANK 'zset "uno") 0)
  (check-false (ZRANK 'non-exist "two"))
  (check-false (ZRANK 'zset "five"))
  (check-equal? (ZREVRANK 'zset "two") 1)
  (check-equal? (ZREVRANK 'zset "one") 0)
  (check-equal? (ZREVRANK 'zset "uno") 2)
  (check-false (ZREVRANK 'non-exist "two"))
  (check-false (ZREVRANK 'zset "five"))
  (check-redis-exn (ZREM 'not-zset "one"))
  (check-equal? (ZREM 'zset "two") 1)
  (check-equal? (ZRANGE 'zset 0 -1 'WITHSCORES)
                (list #"uno" #"1" #"one" #"5"))
  (check-equal? (ZREMRANGEBYRANK 'zset 0 1) 2)
  (check-equal? (ZRANGE 'zset 0 -1 'withscores) null)
  (check-equal? (ZADD 'zset 1 "one") 1)
  (check-equal? (ZADD 'zset 2 "two") 1)
  (check-equal? (ZADD 'zset 3 "three") 1)
  (check-equal? (ZREMRANGEBYSCORE 'zset "-inf" "(2") 1)
  (check-equal? (ZRANGE 'zset 0 -1 'withscores)
                (list #"two" #"2" #"three" #"3"))
  (check-equal? (ZADD 'zset 1 "one") 1)
  (check-equal? (ZREVRANGEBYSCORE 'zset "3" "(1") (list #"three" #"two"))
  (check-equal? (ZREVRANGEBYSCORE 'zset "3" "(1" 'LIMIT 0 1) (list #"three"))
  (check-equal? (ZREVRANGEBYSCORE 'zset "3" "(1" 'LIMIT 1 1) (list #"two"))
  (check-equal? (ZREVRANGEBYSCORE 'zset "3" "(1" 'LIMIT 2 1) (list))
  (check-equal? (ZREVRANGEBYSCORE 'zset "(3" "(1") (list #"two"))
  (check-equal? (ZSCORE 'zset "one") #"1")
  (check-false (ZSCORE 'zset "five"))
  (check-false (ZSCORE 'non-exist 1))
  (check-equal? (ZADD 'zset 4 "four") 1)
  (check-equal? (ZUNIONSTORE 'zdest 2 'zset 'zset2 'WEIGHTS 2 3) 4)
  (check-equal? (ZRANGE 'zdest 0 -1 'WITHSCORES)
                (list #"one" #"5" #"four" #"8" #"two" #"10" #"three" #"15"))
  (check-equal? (ZUNIONSTORE 'zdest 2 'zset 'zset2 'WEIGHTS 2 3
                                                   'AGGREGATE 'SUM) 4)
  (check-equal? (ZRANGE 'zdest 0 -1 'WITHSCORES)
                (list #"one" #"5" #"four" #"8" #"two" #"10" #"three" #"15"))
  (check-equal? (ZUNIONSTORE 'zdest 2 'zset 'zset2 'WEIGHTS 2 3
                                                   'AGGREGATE 'MAX) 4)
  (check-equal? (ZRANGE 'zdest 0 -1 'WITHSCORES)
                (list #"one" #"3" #"two" #"6" #"four" #"8" #"three" #"9"))
  (check-equal? (ZUNIONSTORE 'zdest 2 'zset 'zset2 'WEIGHTS 2 3
                                                   'AGGREGATE 'MIN) 4)
  (check-equal? (ZRANGE 'zdest 0 -1 'WITHSCORES)
                (list #"one" #"2" #"two" #"4" #"three" #"6" #"four" #"8")))

;; misc
(test
 (check-equal? (ECHO "Hello World!") #"Hello World!")
 (check-equal? (PING) "PONG")
 (check-equal? (length (TIME)) 2)
 (check-equal? (car (TIME)) (car (TIME))))

  ;; restore original "dir" and "dbfilename" config parameters 
  (printf "\nTests done. Restoring old parameters: ..........\n")
  (printf "- restoring \"dir\" to: ~a\n" rdb-dir)
  (send-cmd 'config 'set 'dir rdb-dir)
  (printf "- restoring \"dbfilename\" to: ~a\n" rdb-file)
  (send-cmd 'config 'set 'dbfilename rdb-file)
)
