# Contribution Guide

Thank you for investing your time in contributing to our project! Any
contribution you make will be included in future Bytewax releases ✨.

To get an overview of the project, read the
[README](https://github.com/bytewax/bytewax/blob/main/README.md)

Read our [Code of
Conduct](https://github.com/bytewax/bytewax/blob/main/CODE_OF_CONDUCT.md) to
keep our community approachable and respectable.

Read the [Developer Certificate of
Origin](https://github.com/bytewax/bytewax/blob/main/DEVELOPER_CERTIFICATE_OF_ORIGIN.md)
to ensure you are in compliance.

In this guide you will get an overview of the contribution workflow
from opening an issue, creating a PR, reviewing, and merging the PR.

## New To Open Source?

Here are some resources to help you get started with open source
contributions in general:

- [Finding ways to contribute to open source on
  GitHub](https://docs.github.com/en/get-started/exploring-projects-on-github/finding-ways-to-contribute-to-open-source-on-github)

- [Set up
  Git](https://docs.github.com/en/get-started/quickstart/set-up-git)

- [GitHub
  flow](https://docs.github.com/en/get-started/quickstart/github-flow)

- [Collaborating with pull
  requests](https://docs.github.com/en/github/collaborating-with-pull-requests)

## Issues

The Bytewax project keeps a log of work items on GitHub in the [repo's
issues](https://github.com/bytewax/bytewax/issues).

### New Issues

If you spot a problem with Bytewax, [search if an issue already
exists](https://github.com/bytewax/bytewax/issues). If a related issue
doesn't exist, you can open a new issue using a relevant [issue
template](https://github.com/bytewax/bytewax/issues/new/choose).

### Solve an Issue

Scan through our [existing
issues](https://github.com/bytewax/bytewax/issues) to find one that
interests you. You can narrow down the search using `labels` as
filters. By default, we won't assign issues to any contributors that
are not maintainers. If you find an issue to work on, you are welcome
to open a PR with a fix.

Important Labels:

- **Good First Issue** - These are a good entry point for contributing
  to Bytewax.

- **Help Wanted** - These are issues we are looking for the community
  to help contribute to.

## Contribution Steps

### Fork and Clone

[Fork the canonical Bytewax
repository](https://github.com/bytewax/bytewax/fork). Then clone your
fork to your local machine.

### Setup Development Environment

Follow the instructions in [Local
Development](https://docs.bytewax.io/stable/guide/contributing/local-development.html)
to setup your local development environment.

### Modify and Commit

Bytewax is a Python library backed by Rust code. The codebase is split
between Python code in
[`pysrc`](https://github.com/bytewax/bytewax/tree/main/pysrc) and Rust
code in [`src`](https://github.com/bytewax/bytewax/tree/main/src).

The Python code base contains:

- All public API interface definitions.

- Most dataflow operators.

- Most IO connectors.

The Rust code base contains:

- Core Timely runtime.

- Core Bytewax operators.

- [PyO3](https://pyo3.rs/) interface to the runtime.

- Recovery system machinery.

Commit the changes once you are happy with them. Be sure to [sign your
commit](https://docs.github.com/en/organizations/managing-organization-settings/managing-the-commit-signoff-policy-for-your-organization#about-commit-signoffs)
if using the command line interface.

### Pull Request

When you're finished with the changes, create a pull request, also
known as a PR.

- Submit your PR as a "draft" if it is not ready for immediate review.
  Mark it as "ready to review" when ready.

- Don't forget to [link PR to
  issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue)
  if you are solving one.

- Enable the checkbox to [allow maintainer
  edits](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/allowing-changes-to-a-pull-request-branch-created-from-a-fork)
  so the branch can be updated for a merge.

  Once you submit your PR, a maintainer will review your proposal. We
  may ask questions or request for additional information.

- We may ask for changes to be made before a PR can be merged, either
  using [suggested
  changes](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/incorporating-feedback-in-your-pull-request)
  or pull request comments. You can apply suggested changes directly
  through the UI. You can make any other changes in your fork, then
  commit them to your branch.

- As you update your PR and apply changes, mark each conversation as
  [resolved](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/commenting-on-a-pull-request#resolving-conversations).

- If you run into any merge issues, checkout this [git
  tutorial](https://github.com/skills/resolve-merge-conflicts) to help
  you resolve merge conflicts and other issues.

### Merge

Congratulations 🎉🎉 The Bytewax community thanks you! ✨

Once your PR is merged, your contributions will be included in the
next release 😄. A maintainer cuts new releases periodically.
