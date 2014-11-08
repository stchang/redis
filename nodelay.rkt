#lang racket/base

(provide set-tcp-nodelay!)

;; Thanks to Ryan Culpepper
;; http://macrologist.blogspot.ca/2012/03/avoid-flushing-your-wire-protocols.html

(require (rename-in ffi/unsafe [-> ->/f]))

(define IPPROTO_TCP 6)
(define TCP_NODELAY 1)

(define setsockopt_tcp_nodelay
  (get-ffi-obj "setsockopt" #f
    (_fun (socket enabled?) ::
          (socket : _int)
          (_int = IPPROTO_TCP)
          (_int = TCP_NODELAY)
          (enabled-ptr : (_ptr i _int)
                       = (if enabled? 1 0))
          (_int = (compiler-sizeof 'int))
          ->/f (result : _int)
          ->/f (if (zero? result)
                   (void)
                   (error 'set-tcp-nodelay! "failed")))))

(define scheme_get_port_socket
  (get-ffi-obj "scheme_get_port_socket" #f
    (_fun (port) ::
          (port : _racket)
          (socket : (_ptr o _int))
          ->/f (result : _int)
          ->/f (and (positive? result) socket))))

; set-tcp-nodelay! : tcp-port boolean -> void
(define (set-tcp-nodelay! port enabled?)
  (let ([socket (scheme_get_port_socket port)])
    (setsockopt_tcp_nodelay socket enabled?)))
