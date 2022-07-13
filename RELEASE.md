# Steps to Release Bytewax

## 1. One Final PR

Make a PR which commits the following:

1. Bumps version number in `Cargo.toml`

   ```diff
   *** Cargo.toml
    [package]
   -version = "0.1.0"
   +version = "1.2.3"
   ```

2. Commits updated API docs

   ```sh
   (.venv) bytewax/apidocs $ pip install -r requirements.txt
   (.venv) bytewax/apidocs $ rm -rf html/
   (.venv) bytewax/apidocs $ ./build.sh
   ```

   You'll get a warning about `Couldn't read PEP-224 variable
   docstrings`, but ignore that as our PyO3 Rust pyclasses don't have
   Python source `pdoc` can read.

   If we've been committing these as we go, there might not be any
   changes to commit here. That's fine.

3. Labels the latest changelog entries with the version number

   Look in `CHANGELOG.md` for the latest batch of hand-written
   changelog notes and add a new headings with the version number.

   ```diff
   *** CHANGELOG.md
    ## Latest

    __Add any extra change notes here and we'll put them in the release
    notes on GitHub when we make a new release.__

   +## 1.2.3
   +
    * Example note here. Describe any super important changes that you
      wouldn't glean from PR names which will be added by GitHub
      automatically.
   ```

Approve and merge that PR.

4. Check that the CI run completed for the just updated `main` branch
   on [our CI actions
   page](https://github.com/bytewax/bytewax/actions/workflows/CI.yml).

## 2. Create Release on GitHub

Go to the [create a new GitHub release page for our
repo](https://github.com/bytewax/bytewax/releases/new).

1. Choose a tag and enter a tag with the new version number `v1.2.3`.

2. Click "Auto-generate release notes".

   This will pre-populate the GitHub release notes with a list of
   changes via PRs.

3. Copy and paste any hand-written notes from the section of
   [`CHANGELOG.md`](https://raw.githubusercontent.com/bytewax/bytewax/main/CHANGELOG.md)
   with this version into a new section of the GitHub release
   description at the top.

   ```diff
   +## Overview
   +* Paste in the stuff in `CHANGELOG.md` here.
   +
    ## What's Changed
    * List of PRs that were merged, but sometimes the names aren't helpful.
   ```

4. Wait until the CI run above for the `main` branch completes. The
   next CD step needs the wheel packages that are built during CI. It
   looks like this usually takes ~20 min.

5. *Press "Publish release"!*

   This should create a tag in our repo named `v1.2.3` and CD will
   kick off, pushing the final package to PyPI.

   Check that the CD run completed on [our CD actions
   page](https://github.com/bytewax/bytewax/actions/workflows/CI.yml).

## 3. Double check PyPI

Double check our [Bytewax PyPI
page](https://pypi.org/project/bytewax/) to make sure that the new
version of the package is there.

I think we're done! Update this if we're not!
