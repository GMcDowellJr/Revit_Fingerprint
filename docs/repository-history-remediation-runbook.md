# Repository history remediation runbook

Deleting an artifact in a new commit removes it only from the current tree; it
remains available in earlier commits, tags, clones, forks, mirrors, caches, packages,
and release archives. History rewriting therefore requires explicit repository-owner
approval and a coordinated maintenance window.

## Approved planning procedure

1. Inventory affected commits, paths, and literals across every reachable commit,
   branch, and tag with `git log --all -- <sanitized-path>`, `git tag --list`,
   `git show-ref --tags`, `git rev-list --objects --all`, and a local,
   access-controlled content scan. Do not copy discovered values into tickets or logs.
2. Record protected branches, tags, releases, forks, mirrors, automation, caches,
   packages, deployment clones, and commit-SHA integrations that would be affected.
3. Make a disposable mirror (`git clone --mirror <sanitized-url> rewrite.git`),
   disconnect its push URL, and archive its pre-rewrite ref/object inventory. Prepare
   narrow rules and dry-run them only there. A path deletion can use
   `git filter-repo --path path/to/example.dat --invert-paths`; a replacement can use
   `git filter-repo --replace-text sanitized-replacements.txt`, where the file contains
   placeholder literals such as `old-example==>new-example`. Inspect the commit map,
   refs, diffs, and object list before considering any push. Never run these commands
   against the shared repository without owner approval.
4. Review the rewritten commit and tag map. Rewriting invalidates commit signatures
   and signed tags; obtain signer approval and a re-signing plan. Annotated tags have
   tag objects and messages that must be inventoried and deliberately recreated, while
   lightweight tags simply move to rewritten commits. Coordinate the force-push,
   branch-protection exception, CI pause/restart, release replacement, and collaborator
   notification. Existing clones must be freshly cloned or carefully reset; ordinary
   pulls can reintroduce old objects.
5. Contact fork, mirror, backup, cache, package, artifact, release, and hosting
   administrators. A force-push cannot erase independent copies, package versions,
   cached archives, or published releases; each needs an explicit purge, replacement,
   or documented retention decision.
6. Verify all refs with a fresh clone, repeat the controlled commit/path/content
   inventory, inspect tags and releases, run repository tests and hygiene checks, and
   allow server object-retention and garbage-collection procedures to complete. Confirm
   the removed path or literal is absent from reachable refs and a fresh clone, not
   merely absent from the working tree.

If material contains a credential or secret, rotate or revoke it before the rewrite
and verify dependent CI and deployment credentials separately. A rewrite reduces
exposure; it does not make a disclosed credential trustworthy again.

## Execution gate

No rewrite begins until the repository owner approves the exact commit/tag inventory,
sanitized filter rules, signed/annotated-tag treatment, branch-protection exception,
CI pause and restart, force-push window, fork/mirror/cache/package/release response,
communications, fresh-clone verification plan, and retention/garbage-collection
evidence. This document is a verification runbook, not authorization to execute.
