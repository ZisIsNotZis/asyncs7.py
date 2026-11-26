# asyncs7 0.3.1 completion

Verified on 2026-11-26. This release packages the documented read-only client, command-line entry point, and local simulator. The README covers installation, connection lifecycle, supported operations, and errors.

## Verification

- Built the wheel and source distribution from a clean snapshot.
- Installed the wheel into separate Python 3.11.18 and 3.14.4 environments. The installed CLI help and both module imports worked.
- From outside the source checkout, all 23 tests passed on each interpreter with deprecation warnings treated as errors. The tests use synthetic data and loopback.
- The package declares the AGPL-3.0-only SPDX identifier and includes the license file.

Physical controller interoperability is outside this verification. The client supports the read-only operations and limits documented in the README.
