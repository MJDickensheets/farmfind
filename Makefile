server:
	racket server.rkt
lint:
	black scripts
test:
	mypy scripts

