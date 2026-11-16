The CI workflow tests this single compatibility point:

| Component | Pinned selection |
| --- | --- |
| Runner | Ubuntu 22.04, x86_64 |
| Rust | `nightly-2022-07-27`, matching `rust-toolchain` |
| Native C/C++ compiler | GCC/G++ 12.3.0, explicitly selected as `gcc-12` / `g++-12` |
| Python | 3.11.13 |
| redis-py | 3.5.3, from `test/requirements.txt` |
| CMake | 3.31.6, from `requirements.txt` in this directory |
| PD and TiKV | 6.2.0, official Linux amd64 archives verified by `backend.sha256` |
| TiKV storage API | v1, configured in `tikv.toml` and checked against the matrix |

The old TiKV client fork requires the pinned nightly. Python 3.12 removes
`distutils`, which this redis-py version imports; preserve Python 3.11 until a
separate client upgrade is validated. CMake 4 removes old policy compatibility
needed by the locked gRPC build. All dependency-resolving Cargo commands use
`--locked`; format checking does not resolve dependencies. Git dependencies are
public and do not require a private repository token.

Keep `Cargo.lock` at format version 3, which the pinned Cargo can read. The
compatible dependency refresh selects `bytes 1.12.1`, `sha1 0.10.7`, and
`digest 0.10.7` while retaining the locked TiKV fork revision. Avoid unrestricted
`cargo update`: broader dependency or toolchain changes need separate
compatibility validation.

The native build in GitHub Actions run 43728224569 failed under floating GNU
15.2.0/CMake 4 at c-ares' old minimum-policy declaration. CI installs the Ubuntu
22.04 GCC 12 packages and checks both compiler versions before building, and
installs the pinned CMake wheel through the selected Python interpreter.

Actions are pinned by commit with their release versions in comments. The job
uses the runner's rustup directly. PD and TiKV are launched directly, avoiding
floating TiUP/playground versions and installers.

The E2E step logs the runner's initial soft/hard `RLIMIT_NOFILE`, then limits
both to 65,536 for the supervisor and its children. TiKV 6.2.0's startup check
requires 1,000 reserved descriptors plus RocksDB and RaftDB file budgets
(with Titan disabled). The defaults total `1000 + 40960 + 40960 = 82920`,
which failed in hosted run 43729835200. This disposable fixture sets both
`max-open-files` budgets to 1,024, requiring only 3,048 descriptors. TiKV's
system check remains enabled; these smaller engine budgets apply only to CI.

`run_e2e.py` starts a single-replica backend on allocated loopback ports. Each
readiness gate has a 120-second deadline and checks for early process exits.
PD must report healthy, TiKV must register as Up, and Tidis must pass PING and
a transactional SET/GET/DEL probe before the suite starts. The suite has a
15-minute limit. The supervisor terminates and reaps its children on success,
failure, timeout, or termination, printing log tails on exit. Complete service
and suite logs are uploaded even when the E2E step fails.

`e2e_suite.py` runs the existing `test/test_helper.py` entry point with its
discovery and command-line arguments intact, substituting a runner that checks
`TestResult.wasSuccessful()`. This makes assertions and exceptions fail CI
before the separate standalone-helper fix in #112 merges. The helper itself
is unchanged. Rust test targets run even when they discover zero tests;
successful empty targets do not establish regression coverage.

To reproduce locally, install the pinned Python requirements, build with
`cargo build --locked --bin tidis-server`, and extract the checksum-verified
backend archives into a directory containing `pd-server` and `tikv-server`:

```sh
ulimit -Sn 65536
ulimit -Hn 65536
python .github/ci/run_e2e.py --bin-dir /path/to/backend --work-dir /path/to/new-run
```

The work directory must be new; it retains logs and backend state for diagnosis.
Run the limit commands in a subshell if the invoking shell should retain its
original limits. The runner must provide a hard limit of at least 65,536.
