# Library Release Checklist

Here is a process checklist for Bytewax employees to release a new
version of the Python library. Contributors do not need to follow the
steps within and instead should follow the instructions in
<project:#contributing>.

## 1. One Final PR

Make a PR which commits the following:

1. Bumps version number in `Cargo.toml`

   ```{code-block} diff
   :caption: Cargo.toml
    [package]
   -version = "0.1.0"
   +version = "1.2.3"
   ```

2. Commits updated stubs

   ```console
   (dev) $ just stubgen
   ```

   If there was no change to any of our PyO3 Rust API, there might be
   no changes to commit here. People also should be committing these
   changes in the actual feature PRs.

3. Labels the latest changelog entries with the version number

   Look in `CHANGELOG.md` for the latest batch of hand-written
   changelog notes and add a new headings with the version number.

   ```{code-block} diff
   :caption: CHANGELOG.md
    ## Latest

    __Add any extra change notes here and we'll put them in the release
    notes on GitHub when we make a new release.__

   +## 1.2.3
   +
    * Example note here. Describe any super important changes that you
      wouldn't glean from PR names which will be added by GitHub
      automatically.
   ```

4. Write migration guide entry

   Add a section to `docs/user_guide/reference/migration.md` like

   ````{code-block} markdown
   :caption: docs/user_guide/reference/migration.md
   ## From v1.2.2 to v1.2.3

   ### Breaking change to `Cow` API

   The `Cow` API is now named `Bear`.

   If you had code that looked like this

   ```python
   from bytewax.dataflow import Cow

   flow = Cow()
   ```

   It now is should be written as

   ```{testcode}
   from bytewax.dataflow import Bear

   flow = Bear("flow_id")
   ```
   ````

   Then add sub-sections for each of the breaking changes with before
   and after example code.

   You should change any `{testcode}` blocks in the previous versions
   into `python` now that they are no valid code with this new version
   of Bytewax.

Then check before merging:

1. Confirm CI tests pass.

2. Confirm that the documentation for this build renders correctly: go
    to the list of actions at the bottom of the PR, and click on the
    `Details` link next to the last `docs/readthedocs.org:bytewax`
    entry.

Approve and merge that PR.

Check that the CI run completed for the just updated `main` branch on
[our CI actions
page](https://github.com/bytewax/bytewax/actions/workflows/CI.yml)
after merging the PR.

## 2. Create Release on GitHub

Go to the [create a new GitHub release page for our
repo](https://github.com/bytewax/bytewax/releases).

1. Choose a tag and enter a tag with the new version number `v1.2.3`.

2. Click "Auto-generate release notes".

   This will pre-populate the GitHub release notes with a list of
   changes via PRs.

3. Copy and paste any hand-written notes from the section of
   [`CHANGELOG.md`](https://raw.githubusercontent.com/bytewax/bytewax/main/CHANGELOG.md)
   with this version into a new section of the GitHub release
   description at the top.

   ```{code-block} diff
   :caption: GitHub Release Notes Form
   +## Overview
   +* Paste in the stuff in `CHANGELOG.md` here.
   +
    ## What's Changed
    * List of PRs that were merged, but sometimes the names aren't helpful.
   ```

4. Thank every contributor in a `## Contributors` section.

   The auto-generated notes only list PR authors. Also credit
   co-authors and anyone whose code was adopted into another PR. List
   every commit author and `Co-authored-by` trailer since the previous
   release tag:

   ```console
   $ git log v1.2.2..main --format='%aN <%aE>%n%(trailers:key=Co-authored-by,valueonly)' | grep -v '^$' | sort -u
   ```

   Merge duplicate entries for the same person (they may have used
   several emails) and use their GitHub `@handle` where you can.

5. Wait until the CI run above for the `main` branch completes. The
   next CD step needs the wheel packages that are built during CI. It
   looks like this usually takes ~20 min.

6. *Press "Publish release"!*

   This should create a tag in our repo named `v1.2.3` and CD will
   kick off, pushing the final package to PyPI.

   Check that the CD run completed on [our CD actions
   page](https://github.com/bytewax/bytewax/actions/workflows/CD.yml).

## 3. Double check PyPI

Double check our [Bytewax PyPI
page](https://pypi.org/project/bytewax/) to make sure that the new
version of the package is there.

## 4. Double check ReadTheDocs

We host our docs at <https://docs.bytewax.io> which are hosted by
[Read The Docs](https://docs.readthedocs.io/en/stable/index.html). The
RTD project should automatically detect that a new tag was created and
build the documentation from that tag, through the [automation rules
setup here](https://readthedocs.org/dashboard/bytewax/rules/).

Double check that the build for this new version completed at the
[builds page for our
project](https://readthedocs.org/projects/bytewax/builds/). Then go to
our [live production docs](https://docs.bytewax.io) and ensure that
the new release docs are being shown for the `stable` version.

I think we're done! Update this if we're not!

## Release Candidates

A release candidate such as 0.22.0rc1 follows the same steps, with
these differences:

- In the final PR, set the version in `Cargo.toml` to the Cargo
  pre-release form, e.g. `0.22.0-rc.1`. maturin builds the wheels with
  the Python version `0.22.0rc1`.

- Name the tag after the Python version, `v0.22.0rc1`. CD takes the
  version for the Docker images from the tag, and
  `Dockerfile.release` installs that version of `bytewax` from PyPI.

- Label the CHANGELOG notes `## v0.22.0rc1`. For the final release,
  add the final version's heading above any notes added after the
  candidate, and copy the notes under both headings into the GitHub
  release.

- Check "Set as a pre-release" on the GitHub release. CD then pushes
  only the version tags of the Docker images, not the `latest` tags.
  PyPI treats `0.22.0rc1` as a pre-release, so `uv pip install
  bytewax` keeps installing the last final release; testers install
  the candidate with `uv pip install bytewax==0.22.0rc1`.

- On ReadTheDocs, check that `stable` still shows the last final
  release.
