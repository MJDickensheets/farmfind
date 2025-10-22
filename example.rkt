#lang racket/base

(require web-server/servlet
	          web-server/servlet-env)

(define (start request)
    (blog-dispatch request))

(define-values (blog-dispatch blog-url)
    (dispatch-rules
      [("") list-posts]
      [("posts" (string-arg)) review-post]
      [else list-posts]))

(define (list-posts req)
    (response/xexpr `(html (body "list-posts"))))
(define (review-post req p)
    (response/xexpr `(html (body (div "review-post: " ,p)))))

(serve/servlet start
	       #:servlet-regexp #rx""
	       #:port 8080)
