# Contributing

The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

## Getting Started

Recommended way to get started is to create and activate a project virtual environment.
To install package and development dependencies into active environment:

```
$ make install
```

## Testing

To run tests with coverage:

```
$ make test
```

Under the hood `tox` powered by `py.test` and `coverage` configured in `tox.ini` is used.
It's already installed into your environment and could be used separately with more fine-grained control
as described in documentation - https://testrun.org/tox/latest/.

For example to run the tests with increased verbosity. All positional
arguments and options after `--` will be passed to `py.test`:

```
tox -- -v tests/<path>
```
