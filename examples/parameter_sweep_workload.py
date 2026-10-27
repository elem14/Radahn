#!/usr/bin/env python3
"""Radahn parameter-sweep example workload.

Counts primes below N using plain trial division - deliberately
unoptimized so each invocation takes a perceptible, parameter-
dependent amount of CPU time. This is the "real program" in
scripts/parameter_sweep_demo.sh: each Radahn job submitted by that
script runs this same file with a different N, as an independent
process on whichever worker picks it up.

A worker only needs python3 and this file present at the path it is
invoked with - Radahn does not ship, install, or distribute either
one to workers itself.
"""

import sys
import time


def count_primes_below(n: int) -> int:
    count = 0

    for candidate in range(2, n):
        is_prime = True

        for divisor in range(2, int(candidate ** 0.5) + 1):
            if candidate % divisor == 0:
                is_prime = False
                break

        if is_prime:
            count += 1

    return count


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: parameter_sweep_workload.py <n>", file=sys.stderr)
        return 2

    n = int(sys.argv[1])

    start = time.monotonic()
    result = count_primes_below(n)
    elapsed = time.monotonic() - start

    print(f"n={n} primes_below_n={result} elapsed_s={elapsed:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
