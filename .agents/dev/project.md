# Quest Framework

`quest` is a python framework for durable execution.

It uses the principle of *replay* to restore the state of workflows.

It also presents the concept of a workflow surface: a workflow can expose readable state and put-able queues to specified identities (including public). Thus, the information and actions available to the observer are controled temporally within the workflow along with the logic of the workflow. 
