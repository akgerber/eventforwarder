Solution for challenge.

Built on go1.7.4

Accepts events on port 9090. Accepts user clients on port 9099.
Exits upon completion.
Uses go channels as a communication mechanism.

Build with:
`make`

Test with:
`make test`

Run with:
`./main`

Tests require installing 'go-spew' library to prettyprint:
`go get -u github.com/davecgh/go-spew/spew`

