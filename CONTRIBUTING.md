# Welcome to Bytewax contributing guide

Thank you for investing your time in contributing to our project! Any contribution you make will be reflected in Bytewax :sparkles:. 

Read our [Code of Conduct](CODE_OF_CONDUCT.md) to keep our community approachable and respectable.

In this guide you will get an overview of the contribution workflow from opening an issue, creating a PR, reviewing, and merging the PR.

## New contributor guide

To get an overview of the project, read the [README](README.md). Here are some resources to help you get started with open source contributions:

- [Finding ways to contribute to open source on GitHub](https://docs.github.com/en/get-started/exploring-projects-on-github/finding-ways-to-contribute-to-open-source-on-github)
- [Set up Git](https://docs.github.com/en/get-started/quickstart/set-up-git)
- [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow)
- [Collaborating with pull requests](https://docs.github.com/en/github/collaborating-with-pull-requests)


## Getting started

Bytewax is a Python library backed by Rust code. The codebase is split between Python code in [`pysrc/bytewax`](https://github.com/bytewax/bytewax/tree/main/pysrc/bytewax) and Rust code in [`src`](https://github.com/bytewax/bytewax/tree/main/src). PyO3 is used as the interface between the two. Bytewax operators and the execution of dataflows are handled at the Rust layer. The inputs and outputs can be written in either Rust or in Python.

To develop with Bytewax locally you will need to install Rust and use [Maturin](https://github.com/PyO3/maturin) to build the code into a Python module locally. 

### Issues

#### Create a new issue

If you spot a problem with Bytewax, search if an issue already exists. If a related issue doesn't exist, you can open a new issue using a relevant [issue template](https://github.com/bytewax/bytewax/issues/new/choose). 

#### Solve an issue

Scan through our [existing issues](https://github.com/bytewax/bytewax/issues) to find one that interests you. You can narrow down the search using `labels` as filters. As a general rule, we don’t assign issues to anyone. If you find an issue to work on, you are welcome to open a PR with a fix.

Important Labels:
**Good First Issue** - These are a good entry point for contributing to Bytewax.
**Help Wanted** - These are issues we are looking for the community to help contribute to.

### Make Changes

#### Make changes locally

2. Fork the repository.
3. Install Rust (check version)
4. Install Maturin `pip install maturin` ([check version](https://github.com/bytewax/bytewax/blob/main/pyproject.toml))
5. Install dev requirements `pip install -r dev.requirements.txt`
5. Run `Maturin Develop`
6. Test your changes locally.
7. If possible, add a test that will run with CI.

### Commit your update

Commit the changes once you are happy with them. 

### Pull Request

When you're finished with the changes, create a pull request, also known as a PR.
- Fill the "Ready for review" template so that we can review your PR. This template helps reviewers understand your changes as well as the purpose of your pull request. 
- Don't forget to [link PR to issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue) if you are solving one.
- Enable the checkbox to [allow maintainer edits](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/allowing-changes-to-a-pull-request-branch-created-from-a-fork) so the branch can be updated for a merge.
Once you submit your PR, a Docs team member will review your proposal. We may ask questions or request for additional information.
- We may ask for changes to be made before a PR can be merged, either using [suggested changes](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/incorporating-feedback-in-your-pull-request) or pull request comments. You can apply suggested changes directly through the UI. You can make any other changes in your fork, then commit them to your branch.
- As you update your PR and apply changes, mark each conversation as [resolved](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/commenting-on-a-pull-request#resolving-conversations).
- If you run into any merge issues, checkout this [git tutorial](https://github.com/skills/resolve-merge-conflicts) to help you resolve merge conflicts and other issues.

### Your PR is merged!

Congratulations :tada::tada: The Bytewax community thanks you! :sparkles:. 

Once your PR is merged, your contributions will be included in the next release 😄. See the [release process docs](https://github.com/bytewax/bytewax/blob/main/RELEASE.md) for more information on how that is managed. A maintainer will run the release. 
