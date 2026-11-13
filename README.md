# assistant-runner

The Brainstorm Assistants' scheduler. It reads the NIP-FB scheduled tasks (kind 34817) whose
owners named their Brainstorm Assistant as runner, and wakes the task's service when an
occurrence is due. It prints nothing, signs nothing and spends nothing.

The design is [`docs/PLAN.md`](docs/PLAN.md); the trigger contract is [`contract/v1/`](contract/v1/).
