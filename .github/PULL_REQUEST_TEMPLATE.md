## What this changes

<!-- What the change does and why. Link the issue it fixes, e.g. "Fixes #123". -->

## How it was tested

<!-- The tests you added or ran. -->

## Checklist

- [ ] **Both engines:** the change is in Rust and .NET, with the same behaviour, or it only touches one by nature (the CLI, a build file). If you need help with the other language, say so here.
- [ ] **Tests** cover the change, and the formatters, linters and tests in [CONTRIBUTING.md](https://github.com/datalium/strata/blob/main/CONTRIBUTING.md) pass.
- [ ] **Wire format:** unchanged, or the schema in `schema/` has a new minor version and the old one is still readable.
- [ ] **Docs updated?** A change in behaviour updates its `design/` doc and adds a `CHANGELOG.md` entry. If it changes something on [stratamesh.dev](https://stratamesh.dev), say what here, and a maintainer updates the docs site.
- [ ] **Signed off:** every commit has a `Signed-off-by` line (`git commit -s`, see the DCO in CONTRIBUTING.md).
