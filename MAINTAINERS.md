# Maintainers

Bytewax is an open-source project. As of **May 2025**, the original core team
at Bytewax (the company) is no longer maintaining the project full time, and
we are transitioning to community maintenance. This document describes who
maintains the project today, how decisions get made, and how new contributors
can grow into maintainer roles.

If you are a heavy user of Bytewax — especially in production — and would
like to help shape its future, we want to hear from you. The fastest path is
to comment on [#560](https://github.com/bytewax/bytewax/issues/560) or open a
PR; see "Becoming a maintainer" below.

## Current maintainers

| GitHub                                          | Role                  | Areas                                       |
| ----------------------------------------------- | --------------------- | ------------------------------------------- |
| [@awmatheson](https://github.com/awmatheson)    | Project lead (founder) | Project direction, governance, releases    |

> This list is intentionally short while we onboard new community
> maintainers. Active contributors who have shown sustained engagement
> (e.g. recent PRs against `main`) and are interested in a maintainer role
> are encouraged to reach out — see below.

## Emeritus / past core team

Members of the original Bytewax core team who shaped the project but are no
longer maintaining it day to day. Their contributions remain in the codebase
and we are grateful for their work.

- David Selassie ([@davidselassie](https://github.com/davidselassie))
- Dan Herrera ([@whoahbot](https://github.com/whoahbot))
- Federico Dolce ([@Psykopear](https://github.com/Psykopear))
- Laura Gutierrez Funderburk
- Blake Johnson
- Konrad Sienkowski

(If your name belongs on this list and isn't — or shouldn't be — open a PR.)

## What maintainers do

- Triage incoming issues and PRs (apply labels, ask for repro info, close
  duplicates).
- Review and merge pull requests.
- Cut releases and publish wheels to PyPI.
- Steward the public roadmap and make scope calls on feature requests.
- Uphold the [Code of Conduct](./CODE_OF_CONDUCT.md).

Maintainers are expected to be responsive on a best-effort basis — this is
volunteer work, and slow responses are normal. If a PR has been open without
a response for more than two weeks, it is fair to ping in the PR thread or in
the [community Slack](https://join.slack.com/t/bytewaxcommunity/shared_invite/zt-vkos2f6r-_SeT9pF2~n9ArOaeI3ND2w).

## How decisions get made

We aim for **lazy consensus**: substantive proposals (API changes, breaking
changes, new connectors that ship in-tree, dependency bumps that affect the
public ABI) should be opened as a GitHub issue or discussion. If no
maintainer objects within a reasonable window (typically a week), the
proposal is considered accepted.

Day-to-day code changes (bug fixes, internal refactors, documentation) only
need a single maintainer's approval to merge.

For non-trivial disagreements, the project lead has the final say, with the
expectation that this power is used sparingly.

## Becoming a maintainer

We grow the maintainer pool from people who are already contributing. The
rough bar is:

1. **Sustained, substantive contributions.** Roughly three or more merged
   PRs that go beyond typo fixes — bug fixes, features, or meaningful
   reviews — over at least a month.
2. **Good judgment in code review.** Either reviewing others' PRs in
   public, or demonstrating in your own PRs that you understand the
   project's conventions and constraints.
3. **A nomination.** An existing maintainer opens a PR adding you to the
   table above. Other maintainers have one week to object; if none do,
   you're in.

If you want to fast-track this because your team depends on Bytewax, open
an issue introducing yourself and what you'd like to work on. We are
explicitly looking to expand this list.

## Security

Please do not file security issues in public. See
[SECURITY.md](./SECURITY.md) for the disclosure process. If that file does
not yet exist, email the project lead directly.

## Releases

Releases are cut by maintainers with PyPI publish rights. The current
release process lives in [`justfile`](./justfile) and the
[`CD.yml`](./.github/workflows/CD.yml) workflow.
