# Repository history remediation runbook

Deleting an artifact in a new commit removes it only from the current tree; it
remains available in earlier commits, tags, clones, forks, mirrors, caches, and
release archives. History rewriting therefore requires explicit repository-owner
approval and a coordinated maintenance window.

## Approved planning procedure

1. Inventory affected paths and literals across every reachable commit and tag
   with `git log --all -- <path>`, `git rev-list --objects --all`, and a local,
   access-controlled scan. Do not copy discovered values into tickets or logs.
2. Record protected branches, tags, releases, forks, mirrors, automation,
   deployment clones, and commit-SHA integrations that would be affected.
3. In a disposable mirror, prepare path- and content-replacement rules and run
   `git filter-repo` with the narrowest reviewed `--path ... --invert-paths`
   and/or `--replace-text` rules. Never execute those rules against the shared
   repository without owner approval.
4. Review the rewritten commit and tag map, then coordinate the force-push,
   branch-protection exception, CI pause, release replacement, and collaborator
   notification. Every existing clone must be freshly cloned or carefully reset;
   ordinary pulls can reintroduce old objects.
5. Contact fork, mirror, backup, package, artifact, and hosting administrators.
   A force-push cannot erase independent copies or already-published releases.
6. Verify all refs with a fresh clone, repeat the controlled history inventory,
   inspect tags/releases, run repository tests and hygiene checks, and allow
   server garbage-collection/retention procedures to complete.

The repository owner must approve the exact rules, timing, communications, and
post-rewrite evidence before any destructive operation.
