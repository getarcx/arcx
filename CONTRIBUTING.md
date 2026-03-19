# Contributing to ARCX

Thank you for your interest in contributing to ARCX.

## Getting Started

1. Fork the repository
2. Create a feature branch: `git checkout -b my-feature`
3. Make your changes
4. Run the tests: `cargo test`
5. Run the linter: `cargo clippy`
6. Commit your changes with a clear message
7. Push to your fork and open a pull request

## Development Setup

```bash
git clone https://github.com/arcx-archive/arcx.git
cd arcx
cargo build
cargo test
```

## Guidelines

- Follow existing code style. Run `cargo fmt` before committing.
- Add tests for new functionality.
- Keep pull requests focused. One feature or fix per PR.
- Update documentation if your change affects the public API or format specification.
- Format specification changes (FORMAT.md) require discussion in an issue first.

## Reporting Bugs

Open an issue with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- ARCX version (`arcx --version`) and OS

## Format Specification Changes

The ARCX binary format is versioned. Changes to the format require:
1. An issue describing the motivation and proposed change
2. Discussion and approval before implementation
3. Backward compatibility analysis
4. Updates to FORMAT.md

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
