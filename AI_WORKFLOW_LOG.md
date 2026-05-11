# AI Workflow Log

## Tool Used

I used Codex as the AI coding agent inside the local workspace. The assignment PDF was treated as the source of truth, and I asked the agent to implement only the required deliverables.

## Prompting Approach

The initial prompt was: "Act as an interview candidate and complete this assignment." I then let the agent inspect the repository, extract the assignment text from the PDF, and derive a minimal implementation plan from the requirements.

## Key Decisions

- Kept the pipeline local and file-based because the assignment does not require cloud services.
- Used JSON Lines for raw ingestion because it naturally represents nested payloads and partitioned event files.
- Used pandas plus pyarrow for the batch pipeline because the data volume is laptop-sized and Parquet output is mandatory.
- Rewrote managed output directories on each run to guarantee idempotency for identical inputs.
- Filled null `country_code` with `UNKNOWN` but dropped null `device_type`, because country nulls can still be analytically useful while device is a required breakdown key.

## Course Corrections

- The starter generator depended on Faker, which was not installed. The implementation was adjusted to use deterministic standard-library and NumPy generation instead of adding an unnecessary dependency.
- The original starter script duplicated full rows but did not partition output. The final generator writes daily JSONL partitions and preserves duplicate `event_id` values intentionally.
- The assignment warned against extra features, so optional bonuses such as CloudWatch simulation, DuckDB queries, incremental checkpoints, and tests were not added.

## Reflection

The AI agent was most useful for quickly turning the PDF requirements into a concrete implementation checklist and then filling in the pipeline mechanics. The main thing to watch was scope discipline: the assignment explicitly penalizes over-engineering, so the final solution stays close to the requested batch pipeline, outputs, manifest, and documentation.
