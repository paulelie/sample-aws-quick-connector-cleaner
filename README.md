# AWS Quick connector cleaner

Consolidates duplicated Google Workspace and Microsoft 365 action connectors in an AWS Quick
(Enterprise edition) account onto the connector your IT team manages centrally.

Without a central connector, each user creates their own Gmail, Google Calendar, Outlook, ...
connector. Only one connector per service works reliably, and sharing an agent or a flow also means
sharing the creator's private connector. After IT creates the central connector, this script moves every
agent, flow and space onto it and deletes the duplicates. Users who can't use the central connector are
reported and skipped.

A run without `--apply` is a dry run: it only reads from AWS and writes a plan locally.

## Scope

| Suite | Connector types |
| --- | --- |
| Google Workspace | any type whose name contains `GOOGLE` or `GMAIL` (Gmail, Google Calendar, Google Drive, ...) |
| Microsoft 365 | `MICROSOFT_OUTLOOK`, `MICROSOFT_ONEDRIVE`, `MICROSOFT_SHAREPOINT`, `MICROSOFT_TEAMS` |

Change it with `--type-pattern` (a regex on the connector `Type`), for example
`--type-pattern '^MICROSOFT_OUTLOOK$'`.

Connectors are only consolidated within the same type: a Gmail connector never replaces an Outlook one.
Hand-built `GENERIC_HTTP` connectors that call Google or Microsoft APIs are listed for manual review but not
migrated, because their action IDs differ from the managed connector's.

## Requirements

- Python 3.9 or later, and `pip install -r requirements.txt`.
- Credentials for the AWS account that holds the Quick subscription (in an AWS Organization, that's the
  member account with the Quick instance, not the management account), with these permissions:
  - read: `quicksight:ListActionConnectors`, `DescribeActionConnector`, `DescribeActionConnectorPermissions`,
    `ListAgents`, `DescribeAgent`, `DescribeAgentPermissions`, `ListFlows`, `DescribeFlow`,
    `GetFlowPermissions`, `ListSpaces`, `ListSpaceResources`, `ListUserGroups`, `DescribeUser`, plus
    `sts:GetCallerIdentity`
  - write (only with `--apply`): `quicksight:UpdateActionConnectorPermissions`, `UpdateAgent`, `UpdateFlow`,
    `UpdateSpaceResources`, `DeleteActionConnector`; `CreateFlow` and `DeleteFlow` (draft copies, and removing
    a copy that fails its checks); `UpdateAgentPermissions`, `UpdateFlowPermissions` (restoring permissions
    if an update changed them, and keeping draft copies private)

Use read-only credentials for the dry run.

## Usage

```bash
# 1. Dry run. Prompts you to pick the central connector for each type, prints the plan, and writes the
#    announcement messages for the users (with the date of the change).
python quick_connector_cleaner.py --profile quick-admin --region us-east-1 --change-date 2026-11-20

# 2. Same, non-interactive, central connectors given by ID or ARN (one per type).
python quick_connector_cleaner.py --region us-east-1 \
  --canonical <gmail-connector-id> --canonical <calendar-connector-id> --non-interactive

# 3. Apply. Asks you to type "yes" before changing anything.
python quick_connector_cleaner.py --region us-east-1 \
  --canonical <gmail-connector-id> --canonical <calendar-connector-id> --apply

# Re-point everything but keep the duplicates for now.
python quick_connector_cleaner.py --region us-east-1 --canonical <id> --apply --keep-duplicates
```

`--region` is the region of your Quick account.

## How the script works, step by step

The script always runs phase 1. It runs phase 2 only with `--apply`.

### Phase 1: scan and plan (read-only)

**Step 1. Connect.** The script opens a session with `--profile` and `--region` and reads the account ID
from STS (or `--account-id`). It creates the run folder `runs/<timestamp>/`. API calls are retried
automatically when throttled.

**Step 2. Find the connectors.** `ListActionConnectors` returns every connector in the account; deleted
ones are ignored. For each connector whose type is in scope, the script calls `DescribeActionConnector`
(authentication type, endpoints, enabled actions) and `DescribeActionConnectorPermissions` (who owns it
and who it's shared with). `GENERIC_HTTP` connectors are described too, and kept for manual review if
their endpoints point at Google or Microsoft APIs.

**Step 3. Find where each connector is used.** The script reads every agent, flow and space once, and checks
each one against all in-scope connectors in memory. The number of API calls depends on the number of
agents, flows and spaces, not on the number of connectors. The results (including flow definitions) are
kept for the rest of the run. During `--apply`, only a resource that is about to change is read again,
right before the update, so recent edits aren't overwritten.

| Resource | How the connector is found | Owner shown in the plan |
| --- | --- | --- |
| Agent | `DescribeAgent`: the agent's list of action connector ARNs | `DescribeAgentPermissions`, else the agent's `Creator` |
| Flow | `DescribeFlow` for `PUBLISHED` and `DRAFT`: any reference to the connector's ARN or ID in the definition | `GetFlowPermissions`, else the flow's `CreatedBy` |
| Space | `ListSpaceResources`: resources of type `ACTION_CONNECTOR` | the space's creator |

A resource that can't be read is recorded under `scan_errors` in `plan.json`; the scan continues.

**Step 4. Group by type and choose the central connector.** Connectors are grouped by their exact type
(`GMAIL`, `MICROSOFT_OUTLOOK`, ...). For each group with two or more connectors, the central one comes
from `--canonical`. Without it, the script asks you to choose, showing each connector's owner and how many
agents, flows and spaces use it. A group is skipped if it has only one connector, if no central connector
was chosen, or if the chosen one doesn't have status `CREATION_SUCCESSFUL` or `UPDATE_SUCCESSFUL`. Every
other connector in the group is a duplicate.

**Step 5. Pre-check the flows.** For each flow that uses a duplicate, the script lists its connector steps
and their actions, and works out whether the flow could be migrated (see
[Flows](#flows)). A flow that couldn't be migrated is shown as `BLOCKED` with the reason, and listed in
`flows_needing_action.csv`.

**Step 6. Pre-check access.** For each duplicate, the script works out which users must be able to use the
central connector, and whether they can today (see
[Access to the central connector](#access-to-the-central-connector)). Users without access are marked
`will be granted` or `will be skipped`.

**Step 7. Report.** The script prints the plan:
- each type with its central connector and duplicates
- for each duplicate, the agents, flows and spaces that use it, with each flow step's action and any
  `BLOCKED` reason
- the `GENERIC_HTTP` connectors to review by hand
- the impact per user
- the access pre-check

It writes `plan.json`, `access_issues.csv` and `flows_needing_action.csv` to the run folder. In a dry run it
also writes the announcement messages for the impacted users, `user_messages_planned.md` and `.csv` (see
[Messages to users](#messages-to-users)). Without `--apply`, the script stops here.

### Phase 2: apply (`--apply`)

**Step 8. Confirm.** The script shows how many duplicates it will re-point and delete, and asks you to type
`yes`. `--yes` skips the prompt. Without a terminal and without `--yes`, it refuses to run.

**Step 9. Migrate each duplicate.** Duplicates are handled one at a time, in this order:

1. **Back up** the duplicate's configuration and permissions to `backups/connectors/`.
2. **Access gate.** Check that every user of the duplicate can use the central connector, grant access
   where it's missing, read the permissions back and check again. If any user still has no access, this
   duplicate is skipped entirely and the user goes into `access_issues.csv`.
3. **Agents.** Each agent that uses the duplicate is switched to the central connector (see [Agents](#agents)).
4. **Flows.** Each flow is rewritten step by step, verified, and rolled back if the service changed
   anything. A flow whose draft has unpublished edits first gets a draft copy visible to its owners only
   (see [Flows](#flows)).
5. **Spaces.** Each space is switched to the central connector (see [Spaces](#spaces)).
6. **Delete.** Only if every update in 2 to 5 succeeded and `--keep-duplicates` wasn't passed, the
   duplicate is deleted. The script then polls `DescribeActionConnector` for up to 60 seconds: the delete
   counts as done only when the connector is not found or has status `DELETED`.

Every agent, flow and space is backed up to `backups/` before it's changed, and read back after. For agents
and flows, the permissions are checked too and restored if the update changed them (see
[Ownership and sharing](#ownership-and-sharing)).

**Step 10. Finish.** The script writes:
- `results.json`: every step for every duplicate, with its outcome
- `access_issues.csv`: only the users who still have no access
- `flows_needing_action.csv`: the flows skipped and the draft copies created
- `user_messages.md` and `user_messages.csv`: one message per impacted user, in French and English, for you
  to send (see [Messages to users](#messages-to-users))

It prints how many duplicates were migrated and deleted, how many users and flows were skipped, how many
draft copies were created, and how many messages there are to send.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Dry run finished, or every duplicate was migrated |
| `1` | At least one duplicate was kept (see `results.json`), or you didn't confirm |
| `2` | `--apply` without a terminal and without `--yes` |

Running the script again is safe. It scans from scratch, so agents, flows and spaces that were already
migrated no longer reference a duplicate, and only what's left is processed.

## Details

### Access to the central connector

1. **Who.** Every Quick user and group on the duplicate's permissions. With `--grant-scope all`, also the
   owners of the agents, flows and spaces that use it.
2. **Check.** A user has access if they're listed on the central connector's permissions, or belong to a
   Quick group that is (`ListUserGroups`).
3. **Grant.** Users without access are granted the permission set described under `--grant-actions`.
   With `--grant-scope none`, nobody is granted and the script only checks.
4. **Verify.** The central connector's permissions are read back and the check runs again.

If any user still has no access, that duplicate is skipped: its agents, flows and spaces are not
changed, and it is not deleted. Other users' duplicates continue normally. Fix the access, for example by
sharing the central connector with the user or a group they belong to, then run again.

### Agents

`UpdateAgent` removes the duplicate from the agent's action connectors and adds the central one. Name,
description, icon, starter prompts, welcome message and custom prompt are passed through unchanged. The
script waits for the agent to leave `UPDATING`, then checks that the duplicate is no longer attached.

### Flows

A step that calls a connector looks like this:

```json
{"StepId": "abe45a19-...", "StepType": "CONNECTOR_ACTIONS", "StepTitle": "Fetch Unread Emails",
 "StepConfig": {"ConnectorActionsConfig": {"prompt": "...",
   "actionConnectorId": "6f1e98af-...", "actionConnectorType": "GMAIL", "actionId": "ListEmails"}}}
```

An `actionId` is only valid if it's one of the target connector's `EnabledActions`, so the script handles
each such step as a unit:

1. Before the update, it checks that the step's `actionId` is enabled on the central connector and that
   both connectors have the same type. If not, the flow is left unchanged.
2. It sets `actionConnectorId` and `actionConnectorType` together and leaves `actionId`, the prompt and all
   other steps unchanged. If the duplicate is referenced anywhere else in the definition, the flow is
   blocked; the script doesn't guess.
3. It writes the definition with `UpdateFlow`, then reads both versions back. Every connector step must
   still have the same step ID, connector, type and action.
4. If the service changed or dropped anything (for example, it cleared an `actionId`), the script writes
   the original definition back and keeps the duplicate.

#### Draft and published versions

A flow can have up to three versions: `PUBLISHED`, `DRAFT`, and `PENDING_APPROVAL`. The API can't update
them separately. `UpdateFlow` has no version parameter and writes the one definition it's given to both
the draft and the published version, and there's no publish or draft-only operation. So the script reads
all three versions and decides as follows:

| Versions found | What the script does |
| --- | --- |
| Published and draft identical, or only one of them | Rewrites that definition. Both versions are updated. |
| Draft differs from published (unpublished edits) | Creates a draft copy, then updates the original from its published version (see below). |
| A version pending approval | Skips the flow: approve or reject it in Quick first. |

**How the two versions are compared.** Only the `FlowDefinition` returned by `DescribeFlow` for `DRAFT` and
`PUBLISHED`:
- The steps are compared in order. For each step: `StepId`, `StepType`, `StepTitle` and the whole
  `StepConfig` (prompt, connector fields, model settings, ...).
- They differ if a step was added, removed or reordered, if any field of a step changed (one character in a
  prompt is enough), or if any other field of `FlowDefinition` differs.
- Ignored: JSON key order and the metadata outside the definition (`PublishState`, `LastUpdatedTime`,
  `LastUpdatedBy`, `StepAliases`).
- The differences are reported step by step, for example `added: 'Send summary' (3c07...); changed: 'Fetch
  Unread Emails' (abe4...) [prompt]`.

**Draft copy (default, `--flow-divergence copy-draft`).** So that the user's unpublished edits aren't lost:

1. The draft, with its connector steps switched to the central connector, is created as a new flow with
   `CreateFlow`, named `<original name> (draft copy - copie brouillon)`. Its description explains, in French
   and English, where it comes from and what to do with it.
2. It's shared with the original flow's owners only: the users and groups that can delete the flow or change
   its sharing, with exactly their actions. `CreateFlow` always publishes the new flow (the API has no way to
   create a draft only), so sharing it with the owners only is what keeps it private.
3. The copy is read back. Its connector steps must be the ones sent, its step IDs must be unchanged (prompts
   refer to other steps by ID, for example `@abe45a19-...`), and its permissions must be the owners only.
   Anything the service added is revoked. If any check fails, the copy is deleted (`DeleteFlow`) and the flow
   is skipped as with `skip`. Nothing is lost.
4. Only then is the original updated from its published version, with the same checks as any flow. Its draft
   becomes identical to its published version; the edits are now in the copy. If this update fails, the copy
   is kept because it holds the edits.

The copy is listed in `flows_needing_action.csv` (status `draft copied`) and in the owner's message.

**Other choices for `--flow-divergence`:**

- `skip`: leave the flow unchanged. It keeps using the duplicate, so the duplicate is kept. The owner
  publishes or discards the draft in Quick, then you run the script again.
- `use-published`: overwrite both versions with the published one. The unpublished edits are lost; they stay
  in `backups/flows/`.
- `use-draft`: overwrite both versions with the draft. The unpublished edits go live for everyone who runs the
  flow.

A skipped flow is listed in `flows_needing_action.csv` with its owner and the reason.

### Ownership and sharing

The script never changes who owns an agent or flow, or who it's shared with, and it checks that the update
didn't either. Before updating, it saves the permissions (`DescribeAgentPermissions`, `GetFlowPermissions`)
in the backup. After updating, it reads them again:

- **Unchanged:** the step continues.
- **Changed** (for example, an owner removed or the calling IAM role added): the script puts back exactly
  what was removed and revokes exactly what was added (`UpdateAgentPermissions`, `UpdateFlowPermissions`),
  then checks again. A successful restore is logged as a warning.
- **Can't be restored:** the step fails and the duplicate is kept.

One field does change: the script's update is recorded as the last change, so a flow's `LastUpdatedBy` no
longer shows the user. When the script runs as an IAM role that isn't a Quick user (usual with IAM
Identity Center), `LastUpdatedBy` becomes empty and the console may show the role name, for example
"Admin". `CreatedBy` and the permissions stay the same. The backup keeps the original `CreatedBy` and
`LastUpdatedBy` values.

### Spaces

`UpdateSpaceResources` detaches the duplicate and attaches the central connector in one call. The space's
resources are then listed again to confirm the duplicate is gone.

### Messages to users

The script writes one message for each Quick user affected, in French and in English. You send them; the
script doesn't. A user is affected if they own a duplicate or an agent, flow or space that uses one.

| Run | Files | Messages |
| --- | --- | --- |
| Dry run | `user_messages_planned.md`, `user_messages_planned.csv` | Announcements in the future tense ("will be replaced"), predicted from the dry-run plan. Send them before the change. |
| `--apply` | `user_messages.md`, `user_messages.csv` | What was actually done ("was replaced"). Send them after the change. |

In the announcements, the date of the change comes from `--change-date` (for example
`--change-date 2026-11-20`); without it, the messages say `[date à compléter]` / `[date to fill in]`. They
also tell users with unpublished draft edits that they can publish or discard their draft before that date
to avoid the draft copy. Announcements are predictions: they assume the account doesn't change before
`--apply`, and agent and space updates are assumed to succeed (they can't be checked in advance). Always
send the `--apply` messages afterwards.

Each message says, in plain words:

- which of their connectors were replaced by the shared connector and deleted, or kept and why
- which agents, flows and spaces now use the shared connector
- which flows had unpublished draft changes, and the name of the copy that holds them (visible only to them)
- what wasn't changed yet and why (no access to the shared connector, a version waiting for approval, an
  action not enabled yet, or a failed check), with what happens next
- what they need to do: sign in once to the shared connector, and review their draft copies

`user_messages.md` has one section per user (French, then English), easy to read or copy into an email.
`user_messages.csv` has the same content for a mail merge: `user`, `email`, `subject_fr`, `body_fr`,
`subject_en`, `body_en`, `screenshots`, `admin_notes`. The email comes from `DescribeUser`; if it's empty, the user name is
used when it looks like an email address (usual with IAM Identity Center). `admin_notes` holds the technical
details (error messages, the draft changes found) for you; don't send it to users.

**Screenshots.** The messages include screenshots of the Quick console, taken after the change, from
`assets/screenshots/`:

| File | Shown with | Content |
| --- | --- | --- |
| `01-more-connectors-menu.png` | the sign-in step | The **More > Connectors** menu, with Connectors circled |
| `02-connectors-shared.png` | the sign-in step | The shared connectors on the Connectors page (example: Gmail, Google Calendar) |
| `03-connector-sign-in.png` | the sign-in step | A shared connector's page, "Not signed in", with **Sign in** circled |
| `04-flows-draft-copy.png` | the draft copy section | The Flows list with a draft copy, circled. **Illustration**: the copy row was added to the page for the screenshot, nothing was created. The owner names are blurred |

In `user_messages*.md`, they appear as images next to the matching text, and the script copies them to
`screenshots/` in the run folder so the file is self-contained. In `user_messages*.csv`, the `screenshots`
column lists the files to attach, and the bodies say where each one applies.

They were taken in a test account with the console in English, and use Gmail as the example. Private
values are blurred in place, so users still see where they appear: the Quick account name (top right) in
all four, the user's first name in the greeting (screenshot 1), and the owner names in the flows list
(screenshot 4). The duplicate connectors were hidden to show the state after the change. To use your own,
replace the files in `assets/screenshots/` with the same names, and blur your account name, user names and
any email address before sending them to users. Nothing else needs to change.

Read the messages before sending them. The sign-in step quotes the console labels in English (**More >
Connectors**, **Sign in**); check them against what your users see.

## Output files

Each run writes to `runs/<timestamp>/`:

- `plan.json`: every connector in scope, its references, the per-user impact and the access pre-check.
- `access_issues.csv`: users of a duplicate who can't use the central connector. In a dry run it lists
  everyone without access today, with status `will be granted` or `will be skipped`. After `--apply` it
  lists only the users who still have no access (status `skipped`). Columns: `user`, `status`, `reason`,
  the duplicate and central connector IDs and names, the connector type, and the affected `agents`,
  `flows` and `spaces`.
- `flows_needing_action.csv`: flows that need the owner's attention. Statuses: in a dry run, `will be
  skipped` or `draft copy will be created`; after `--apply`, `skipped` (flow unchanged, duplicate kept),
  `draft copied`, or `draft copied, original not migrated`. Typical reasons for a skip: a version pending
  approval, an action that isn't enabled on the central connector, or a draft copy that failed its checks.
  Columns: `flow_id`, `flow_name`, `owners`, `status`, `reason`, `draft_changes`, `copy_flow_id`,
  `copy_flow_name`, and the duplicate and central connector IDs and names.
- `user_messages_planned.md`, `user_messages_planned.csv` (dry run): announcements to send to the users before
  the change. See [Messages to users](#messages-to-users).
- `screenshots/`: the console screenshots used in the messages (copied from `assets/screenshots/`).
- `user_messages.md`, `user_messages.csv` (with `--apply`): the messages describing what was done, to send
  after the change.
- `results.json` (with `--apply`): each step taken for each duplicate and its outcome.
- `backups/` (with `--apply`): what each resource looked like before it was changed. Agents: the agent and
  its permissions. Flows: both versions (definition, `CreatedBy`, `LastUpdatedBy`, ...) and the
  permissions. Spaces: their resources. Duplicate connectors: configuration and permissions.

## Options

| Option | Default | Effect |
| --- | --- | --- |
| `--canonical ID_OR_ARN` | prompt | The legitimate connector for its type. Repeat once per type. |
| `--non-interactive` | off | Never prompt. Types without `--canonical` are skipped. |
| `--apply` | off | Make the changes. |
| `--yes` | off | Skip the confirmation prompt (needed when there is no terminal). |
| `--keep-duplicates` | off | Re-point dependencies but do not delete duplicates. |
| `--grant-scope` | `duplicate` | Who must have access to the central connector, and is granted it if missing: the duplicate's principals (`duplicate`), those plus the owners of affected agents, flows and spaces (`all`). With `none`, nobody is granted access and the duplicate's principals are only checked. |
| `--grant-actions a,b` | inferred | Permission actions to grant (at most 20). By default the script copies the smallest non-owner permission set it finds on the central connector, then on other connectors. Only Quick user and group ARNs are granted; action connectors can't be shared with a namespace or an account root. |
| `--flow-divergence` | `copy-draft` | What to do when a flow's draft has unpublished edits. `copy-draft` keeps them in a copy visible to the owners only and updates the original from its published version. `skip` leaves the flow unchanged. `use-published` loses the draft edits; `use-draft` publishes them. See [Draft and published versions](#draft-and-published-versions). |
| `--type-pattern REGEX` | `GOOGLE\|GMAIL\|^MICROSOFT_` | Which connector types are in scope. |
| `--change-date TEXT` | placeholder | Dry run: the date of the planned change, written into the announcement messages. |
| `--output-dir DIR` | `runs` | Where plans, results and backups go. |
| `--profile`, `--region`, `--account-id` | | AWS profile, Quick region (required), account ID (default: from STS). |
| `-v`, `--verbose` | off | Debug logging, including the AWS SDK. |

## Limits

- `DeleteActionConnector` cannot be undone. The backups hold the configuration and permissions for a
  manual re-creation, but not the OAuth secrets or the users' tokens.
- After the run, each user signs in once to the central connector (per-user OAuth) before their agents
  and flows can use it.
- The script only sees agents, flows and spaces that the `List*` calls return to the calling identity.
  Compare the counts in the log with what you expect, and run the dry run again after `--apply`: a clean
  account shows one connector per type.
- Something created between the scan and the delete is not covered. Run during a quiet period, or use
  `--keep-duplicates` first and delete in a second run.

## API calls

Each call was checked against the
[Amazon QuickSight API Reference](https://docs.aws.amazon.com/quicksight/latest/APIReference/):
`ListActionConnectors`, `DescribeActionConnector`, `DescribeActionConnectorPermissions`,
`UpdateActionConnectorPermissions`, `DeleteActionConnector`, `ListAgents`, `DescribeAgent`,
`DescribeAgentPermissions`, `UpdateAgent`, `ListFlows`, `DescribeFlow`, `GetFlowPermissions`, `UpdateFlow`,
`ListSpaces`, `ListSpaceResources`, `UpdateSpaceResources`, `ListUserGroups`, `UpdateFlowPermissions`,
`UpdateAgentPermissions`, `CreateFlow`, `DeleteFlow`, `DescribeUser`.

**Pagination.** The paginated calls are `ListActionConnectors`, `ListAgents`, `ListFlows`, `ListSpaces` and
`ListUserGroups`. The script follows `NextToken` until the last page, asking for the largest page size each
allows (100, and 500 for `ListSpaces`). An empty page that still has a `NextToken` doesn't end the loop.
If the service returns the same `NextToken` twice, the run stops with an error instead of acting on an
incomplete list. During `--apply`, a group lookup that hits this error skips that user's duplicate. The
other calls aren't paginated: `ListSpaceResources` returns all of a space's resources in one response, and
permission lists hold at most 64 entries.

## Assumptions to check on a test account first

Confirmed on a real account, read-only: a Gmail connector reports type `GMAIL`, its `EnabledActions` use the
same names as a flow step's `actionId`, and a flow's connector step has the format shown under
[Flows](#flows). Asking `DescribeFlow` for a version that doesn't exist returns `ValidationException`
("appVersion is not found for the stage ..."), not `ResourceNotFoundException`; the script treats only
that message as "version missing". Everything else was tested against an in-memory fake of the API. Still to check:

- Other Google types are matched on `GOOGLE`, because the SDK enum doesn't list them yet.
- Flow definitions use an internal format that the API docs say may change, and only
  `CONNECTOR_ACTIONS` steps have been seen so far.
- That `UpdateAgent` keeps fields that are passed through unchanged.
- Draft copies (`CreateFlow`) haven't run on a real account. Check that the copy keeps the step IDs (otherwise
  the script deletes it and skips the flow), that only the owners can see it, and that it doesn't inherit a
  schedule from the original.
- The permission action names for a connector "user", which the script copies from existing permissions
  instead of hard-coding.
- One `--apply` has been run on a real account: one flow re-pointed from a duplicate Gmail connector, then
  the duplicate deleted. The flow's owner and sharing were unchanged afterwards, and `LastUpdatedBy` became
  empty. Agents and spaces haven't been updated on a real account yet.
