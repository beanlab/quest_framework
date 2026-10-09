{{ read_file(rosters() + '/agents/general-agent-behavior.md') }}

{{ read_file('dev/project.md') }}

# `myteam`

We use `myteam` as our agent skills and workflow harness. Load a skill when it directly applies to the task at hand; don't load a skill just because it involves the same system or domain as the task.

{{ myteam_explain() | increase_headers(1) }}

## General skills

{{ myteam_list(rosters() + '/agents/skills') }}

