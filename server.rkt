#lang racket/base

(require json
         racket/string
         web-server/servlet
         web-server/servlet-env
         web-server/configuration/responders)

(define (start request)
  (app-dispatch request))

(define-values (app-dispatch app-url)
  (dispatch-rules
    [("") homepage]
    [("state-geo") (static-json-file "states.json")]
    [("county-geo") (static-json-file "counties.json")]
    [("style.css") (lambda (_) (file-response 200 #"OK" "style.css"))]
    [("leaflet.css") (lambda (_) (file-response 200 #"OK" "leaflet.css"))]
    [("leaflet.js") (lambda (_) (file-response 200 #"OK" "leaflet.js"))]
    [("app.js") (lambda (_) (file-response 200 #"OK" "app.js"))]
    [else homepage]))

(define (homepage req)
  (define site-name
    (string-join (list "FarmFinder"
                       (number->string (* (random 2 10) 1000))
                       "\U1F33D")))
  (response/xexpr `(html (head (title (unquote site-name))
                               (link ((rel "stylesheet")
                                      (href "/style.css")
                                      (type "text/css")))
                               (link ((rel "stylesheet")
                                      (href "/leaflet.css")
                                      (type "text/css")))
                               (script ((src "/leaflet.js"))))
                         (body (h1 (unquote site-name))
                               (div ((id "map")))
                               (script ((src "/app.js")))))))

(define (static-json-file file-name)
  (lambda (req)
    (response/jsexpr
     (read-json
       (open-input-file (build-path (current-directory)
            file-name))))))

(serve/servlet start
         #:servlet-path "/"
         #:servlet-regexp #rx""
         #:server-root-path (current-directory)
         #:port 8080
         #:extra-files-paths
         (list (build-path (current-directory) "htdocs")))
