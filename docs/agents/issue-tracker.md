# Issue tracker: GitHub

Issues and specs for this project live in GitHub Issues at https://github.com/Lancetnik/agent-hub. Use the `gh` CLI for all operations.

Pass `--repo Lancetnik/agent-hub` to every `gh issue`, `gh pr`, and `gh label` command. For `gh api`, use `repos/Lancetnik/agent-hub/...` explicitly. Keep repository selection explicit rather than depending on the current working directory.

## Conventions

- Write issue titles, bodies, and comments in English.
- **Create an issue**: `gh issue create --repo Lancetnik/agent-hub --title "..." --body-file <file>`. For multiline bodies, write the exact Markdown to a temporary file and pass it with `--body-file`.
- **Read an issue**: `gh issue view <number> --repo Lancetnik/agent-hub --json number,title,body,labels,comments`.
- **List issues**: `gh issue list --repo Lancetnik/agent-hub --state open --json number,title,body,labels,comments`, with appropriate `--label` and `--state` filters. Paginate or raise the limit when a complete queue is needed.
- **Make an issue a sub-issue of a parent**: obtain the child's database id with `gh api repos/Lancetnik/agent-hub/issues/<child> --jq .id`, then use `gh api --method POST repos/Lancetnik/agent-hub/issues/<parent>/sub_issues -F sub_issue_id=<child-db-id>`. Where sub-issues are unavailable, put `Part of #<parent>` at the top of the child body and link the child in the parent's task list.
- **Comment on an issue**: `gh issue comment <number> --repo Lancetnik/agent-hub --body-file <file>`.
- **Apply / remove labels**: `gh issue edit <number> --repo Lancetnik/agent-hub --add-label "..."` / `--remove-label "..."`. Use the role mapping in `triage-labels.md`.
- **Close**: `gh issue close <number> --repo Lancetnik/agent-hub`. Post any closing explanation first as an issue comment.

## Pull requests as a triage surface

**PRs as a request surface: no.**

When set to `yes`, PRs use the same labels and states as issues:

- **Read**: `gh pr view <number> --repo Lancetnik/agent-hub --comments` and `gh pr diff <number> --repo Lancetnik/agent-hub`.
- **List external PRs**: `gh api --paginate 'repos/Lancetnik/agent-hub/pulls?state=open' --jq '.[] | select(.author_association | IN("OWNER","MEMBER","COLLABORATOR") | not) | {number, title, author: .user.login, author_association, labels: [.labels[].name]}'`.
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label` / `--remove-label`, `gh pr close`, each with `--repo Lancetnik/agent-hub`. Use `--body-file` for multiline comments.

GitHub shares one number space across issues and PRs. When the referenced surface is ambiguous, resolve with `gh pr view <number> --repo Lancetnik/agent-hub` and fall back to `gh issue view` with the same repository.

## When a skill says "publish to the issue tracker"

Create a GitHub issue in `Lancetnik/agent-hub`.

## When a skill says "fetch the relevant ticket"

Read the issue using the command above.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a single issue with **child** issues as tickets.

- Research results live in full issue comments with primary-source links. Dedicated research branches are optional and may be removed after the findings are preserved in the issue.
- **Map**: a single issue labelled `wayfinder:map`, holding the Notes / Decisions-so-far / Fog body.
- **Child ticket**: an issue linked to the map as a GitHub sub-issue. Where sub-issues are unavailable, add the child to a task list in the map body and put `Part of #<map>` at the top of the child body. Labels: `wayfinder:<type>` (`research` / `prototype` / `grilling` / `task`). Once claimed, the ticket is assigned to the driving dev.
- **Blocking**: use GitHub's native issue dependencies. Add an edge with `gh api --method POST repos/Lancetnik/agent-hub/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`. Obtain the blocker's numeric database id with `gh api repos/Lancetnik/agent-hub/issues/<blocker> --jq .id`; it is distinct from the issue number and node id. The live count of open blockers is `issue_dependencies_summary.blocked_by`. Where dependencies are unavailable, use a `Blocked by: #<n>, #<n>` line at the top of the child body. A ticket is unblocked when every blocker is closed.
- **Frontier**: collect the map's open children from its sub-issues or task list, drop any with an open blocker or an assignee, and select the first in map order.
- **Claim**: `gh issue edit <number> --repo Lancetnik/agent-hub --add-assignee @me`, the session's first write.
- **Resolve**: post the answer as an issue comment, close the ticket, then append a context pointer (gist + link) to the map's Decisions-so-far.
