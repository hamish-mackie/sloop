# Contract Tests

The default test suite stays green. Tests marked `red contract` specify the
next unimplemented behavior and are ignored until that implementation starts.

Run the pending `post` contracts with:

```sh
cargo test --test post -- --ignored
```

Run the pending worker CLI contracts with:

```sh
cargo test --test worker_verbs -- --ignored
```

These commands are expected to fail against the current stub. When implementing
a contract, remove its `ignore` attribute and make it pass in the default suite.
