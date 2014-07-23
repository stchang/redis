#lang scribble/manual
@(require scribble/eval
          (for-label redis
                     racket/contract/base
		     racket/async-channel
                     racket))

@title{A Racket Redis client}

@defmodule[redis]

A @hyperlink["http://redis.io/"]{Redis} client for Racket.

@(define the-eval (make-base-eval))
@(the-eval '(require redis racket/async-channel))

@author[@author+email["Stephen Chang" "stchang@racket-lang.org"]]
@author[@author+email["Marc Burns" "m4burns@csclub.uwaterloo.ca"]]

@; connection pool ------------------------------------------------------------
@section{Getting a Connection: Connection Pools}

To connect to a Redis database, first create a connection pool, then
lease a connection from the pool. 

@defproc[(redis-connection-pool? [pool any/c]) bool/c]{
  Indicates whether @racket[pool] is a Redis connection pool.}

@defproc[(redis-connection? [conn any/c]) bool/c]{
  Indicates whether @racket[conn] is a Redis connection.}

@defproc[(make-connection-pool 
          [#:host host string? "127.0.0.1"]
          [#:port port (integer-in 1 65535) 6379]
	  [#:max-connections max exact-nonnegative-integer? 100]
	  [#:max-idle max-idle exact-nonnegative-integer? 10]) 
         redis-connection-pool?]{
  Makes a Redis connection pool with the given parameters.

  A connection pool maintains one connection per thread. Thus, a new
connection is created on the first call to
@racket[connection-pool-lease] in a thread. Calling
@racket[connection-pool-lease] a second time from the same thread will
return the same connection as the first lease call (as long as the
connection was not returned in between).}

@defproc[(kill-connection-pool [pool redis-connection-pool?]) void?]{
  Kills a connection pool. All the connections are disconnected.}

@defproc[(connection-pool-lease 
          [pool redis-connection-pool? (current-redis-pool)])
         redis-connection?]{
  Leases a connection from the specified pool. @racket[current-redis-pool] is 
used if no pool is given.

  The pool maintains one connection per thread. Therefore, if no connection
exists for the current thread, then a new connection is created. If a
connection already exists for the current thread, then that connection is
returned.
@examples[#:eval the-eval
(define pool (make-connection-pool))
(eq? (connection-pool-lease pool) (connection-pool-lease pool))
(eq-hash-code (connection-pool-lease pool))
(define ch (make-async-channel))
(thread (lambda () (async-channel-put ch (eq-hash-code (connection-pool-lease pool)))))
(async-channel-get ch)]
}

@defproc[(connection-pool-return 
          [conn redis-connection?]
          [pool redis-connection-pool? (current-redis-connection)])
         void?]{
  Returns a connection to the specified pool. @racket[current-redis-pool] is
used if no pool is given.

  UNSUBSCRIBE, PUNSUBSCRIBE, and UNWATCH are called on the channel before it's 
returned to the pool.

Returning an already returned pool does nothing.}

@defparam[current-redis-pool pool redis-connection-pool? #:value #f]{
  Represents the current connection pool.

  The parameter is initially false, but gets set if @racket[send-cmd]
or @racket[send-cmd/no-reply] is called with no connection argument.}


@; sending commands: general --------------------------------------------------
@section{Sending Commands: General}

@defproc[(send-cmd [#:rconn conn bool/c #f]
		   [#:host host string? "127.0.0.1"]
		   [#:port port (integer-in 1 6535) 6379]
		   [cmd (or/c string? bytes? symbol?)]
                   [arg (or/c string? bytes? symbol? number?)] ...)
         any/c]{
  Sends the given command, with the given arguments, using the given connection
parameters. Waits for a reply from Redis and returns it.

@itemlist[
 @item{The @racket[conn] connection is used if given.}
 @item{Otherwise a new connection is leased from the 
       @racket[current-redis-pool].}
 @item{If no pool exists, then a new one is created using the given 
       @racket[host] and @racket[port] and set as the 
       @racket[current-redis-pool].}]

A command can either be a string, a byte string, or a symbol. An arguments can
take the form of a string, byte string, symbol, or number.

@examples[#:eval the-eval
(define pool (make-connection-pool))
(define conn (connection-pool-lease pool))
(send-cmd #:rconn conn 'dbsize)]
}

@defproc[(send-cmd/no-reply [#:rconn conn bool/c #f]
                       	    [#:host host string? "127.0.0.1"]
			    [#:port port (integer-in 1 6535) 6379]
			    [cmd (or/c string? bytes? symbol?)]
			    [arg (or/c string? bytes? symbol? number?)] ...)
         redis-connection?]{
  Same as @racket[send-cmd] except does not wait for a reply. The connection
used to send the command is returned.}

@defproc[(get-reply [conn (connection-pool-lease (current-redis-pool))]) 
         any/c]{
  Gets a reply from Redis using the given connection.
@examples[#:eval the-eval
(define pool (make-connection-pool))
(define conn (connection-pool-lease pool))
(send-cmd #:rconn conn 'set 'testing 101)
(get-reply (send-cmd/no-reply #:rconn conn 'get 'testing))]
}

@defproc[(get-reply-evt [conn (connection-pool-lease (current-redis-pool))]) 
         evt?]{
  Returns an event that is ready for synchronization when the given connection
has a reply.}

@; sending commands: specific -------------------------------------------------
@section{Sending Commands: Specific}

Functions for specific Redis commands are also defined. These functions
have the same arguments as @racket[send-cmd], minus the @racket[cmd] argument.

Check the Redis documentation for what return values to expect.

@itemlist[
 @item{The following functions are available:
APPEND DEL GETSET MGET SETRANGE STRLEN EXISTS MSETNX RENAMENX GET GETRANGE RANDOMKEY SET MSET RENAME DUMP RESTORE SETBIT GETBIT BITCOUNT BITOP LPUSH LPUSHX LRANGE LLEN LREM RPUSH RPUSHX LINDEX LPOP RPOP RPOPLPUSH BLPOP BRPOP BRPOPL PUSH LINSERT LSET LTRIM HDEL HGETALL HINCRBY HINCRBYFLOAT HMGET HVALS HMSET HSET HEXISTS HSETNX HGET SADD SCARD SDIFF SDIFFSTORE SINTER SINTERSTORE SMEMBERS SREM SUNION SUNIONSTORE SISMEMBER SMOVE SPOP SRANDMEMBER ZADD ZCARD ZCOUNT ZINCRBY ZINTERSTORE ZRANGE ZRANGEBYSCORE ZREM ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZUNIONSTORE ZRANK ZREVRANK ZSCORE DECR DECRBY INCR INCRBY INCRBYFLOAT HKEYS HLEN EXPIRE EXPIREAT PERSIST PEXPIRE PEXPIREAT TTL PTTL MULTI DISCARD WATCH UNWATCH EXEC SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE PUBLISH DBSIZE ECHO FLUSHALL FLUSHDB KEYS PING TIME TYPE AUTH QUIT SELECT}

 @item{The following GET variations, which return different types of results, are defined:
GET/str GET/num GETRANGE/str GET/list GET/set GET/hash GET/heap
@examples[#:eval the-eval
(SET 'testing2 101)
(GET 'testing2)
(GET/str 'testing2)
(GET/num 'testing2)]
}

 @item{The following SET variations, which allow different types of input, are defined: SET/list SET/set SET/hash SET/heap}

 @item{The following hash variations are defined: HGET/str}

 @item{The following POP variations are defined: POP/list}
 @item{@racket[(do-MULTI c ...)] wraps @racket[MULTI] and @racket[EXEC] around the given commands.}

@item{The following functions are not currently available: 
@itemlist[
 @item{SETNX, SETEX, PSETEX (use SET + options instead)}
 @item{BGREWRITEAOF BGSAVE INFO LASTSAVE MIGRATE MONITOR MOVE SAVE SHUTDOWN SLAVEOF SLOWLOG SYNC CLIENT CONFIG DEBUG EVAL EVALSHA SCRIPT}
]}
 
]


@; pubsub-specific connections ------------------------------------------------
@section{PubSub-specific Connections}

  To help manage pubsub patterns, this client supports 
@deftech{pubsub-specific connection}s.

@defproc[(pubsub-connection? [conn any/c]) bool/c]{
  Indicates whether a connection is a @tech{pubsub-specific connection}.}

@defproc[(lease-pubsub-conn [pool redis-connection-pool? (current-redis-pool)])
         pubsub-connection?]{
  Leases a @tech{pubsub-specific connection} from the pool.

Creates a thread that monitors the connection for (p)subscribe and
(p)unsubscribe messages. Currently prints (un)subscription notices to stdout.

A custodian is created to manage the threads of the pubsub connection.}

@defproc[(make-subscribe-chan 
          [conn pubsub-connection?]
          [key (or/c string? symbol?)]
 	  [pool redis-connection-pool? (current-redis-connection)]
	  [#:psubscribe psub? bool/c #f]) async-channel?]{
  (P)Subscribes to a specified key or pattern. Returns an async-channel where
subscription messages are sent.

Creates a thread that monitors the connection for subscription messages.

To (p)unsubscribe from a subscription, use @racket[send-cmd] or @racket[UNSUBSCRIBE], or @racket[PUNSUBSCRIBE] with the appropriate 
@tech{pubsub-specific connection}.

@examples[#:eval the-eval
(define pool (make-connection-pool))
(define pubsubconn (lease-pubsub-conn pool))
(redis-connection? pubsubconn)
(pubsub-connection? pubsubconn)
(define foo-chan (make-subscribe-chan pubsubconn 'foo pool))
(define bar-chan (make-subscribe-chan pubsubconn 'bar pool))
(sleep .1)
(thread (λ () (PUBLISH 'foo "Hello")))
(thread (λ () (PUBLISH 'bar "World!")))
(async-channel-get foo-chan)
(async-channel-get bar-chan)
]
}

@defproc[(return-pubsub-conn
          [conn pubsub-connection?]
	  [pool redis-connection-pool? (current-redis-connection)])
         void?]{
  Returns the @tech{pubsub-specific connection} to the pool.

Calls @racket[custodian-shutdown-all] on the custodian associated with the 
connection, killing all the threads associated with the connection.}

@; errors ---------------------------------------------------------------------
@section{Errors}
@defproc[(exn:fail:redis? [exn any/c]) bool/c]{
  Identifies a Redis exception.}

@defproc[(redis-error [msg string?]) exn:fail:redis?]{
  Raises a Redis exception with the given message.}

@; basic connect/disconnect ---------------------------------------------------
@section{Raw Connection}

NOTE: It's not recommended to use the forms in this section.

The functions in this section enable basic connection and
disconnection from a Redis database. The user is responsible for
managing the connection.

In particular, using the same connection in different threads may require
some synchronization when sending commands and checking replies.

@defproc[(connect [host string? "127.0.0.1"] [port (integer-in 1 65535) 6379])
         redis-connection?]{
  Connects to a Redis database at host @racket[host] and port @racket[port]. If
no host and port is provided, then @racket[localhost] and 6379 are used, 
respectively.}

@defproc[(disconnect [conn redis-connection?]) void?]{
  Disconnects a Redis connection.}

@defstruct*[redis-connection ([in input-port?]
                              [out output-port?]
                              [owner thread?]
                              [pubsub? bool/c])]{
  Redis connection data structure. The @racket[owner] field is mutable.
  The @racket[pubsub?] field is mutable and optional.}
