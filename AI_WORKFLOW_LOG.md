# AI Workflow Log

## AI Tool Used
- ChatGPT (GPT-5.5)
- GitHub Copilot (optional, if you used it)

---

# Objective

The goal of this assignment was to build a local batch data pipeline for synthetic event data generation, cleaning, transformation, aggregation, and Parquet-based analytical outputs while strictly adhering to assignment scope and avoiding over-engineering.

The AI assistant was used as a development partner for:
- architecture planning
- schema design
- implementation scaffolding
- transformation logic
- aggregation design
- documentation support
- review and simplification

All generated code was manually reviewed and validated before being included in the final solution.

---

# Development Approach

The implementation was intentionally broken into small stages instead of attempting to generate the entire solution in a single prompt.

The workflow followed these phases:

1. Requirement analysis
2. Schema design
3. Synthetic data generation
4. Data cleaning and validation
5. Transformations
6. Aggregations
7. Storage and partitioning
8. Manifest and observability
9. Documentation and review

This staged approach helped maintain clarity and prevented AI drift.

---

# Prompting Strategy

The prompting strategy focused heavily on:
- explicit constraints
- modular development
- avoiding unnecessary abstractions
- maintaining readability

Example prompts included:
- “Design a realistic event schema for a SaaS/web analytics platform.”
- “Generate synthetic event data with controlled data quality issues.”
- “Implement modular pandas transformations for session classification.”
- “Write aggregation logic using groupby operations only.”
- “Refactor the solution to avoid over-engineering.”

Rather than asking for a complete solution upfront, prompts were scoped narrowly to individual pipeline components.

---

# Key AI-Assisted Decisions

## 1. Schema Design

AI was used to iterate on the event schema until it supported:
- session-based analysis
- funnel calculations
- device/country aggregations
- flexible payload attributes

A nested payload structure was selected to allow extensibility while keeping the top-level schema stable.

---

## 2. Data Generation

AI assisted with:
- realistic event distributions
- reproducible random seeding
- controlled insertion of:
  - null values
  - duplicate IDs
  - malformed timestamps

Additional manual validation was performed to confirm the generated dataset satisfied assignment percentages.

---

## 3. Cleaning and Validation

AI generated initial cleaning functions for:
- deduplication
- timestamp parsing
- bot filtering
- null handling

The logic was simplified manually to improve readability and maintain deterministic behavior.

---

## 4. Transformations

AI helped implement:
- payload flattening
- date/hour derivation
- session duration classification
- intra-day event ranking

The session classification thresholds were manually chosen and documented after reviewing generated outputs.

---

## 5. Aggregations

AI was used to scaffold aggregation logic for:
- daily_user_summary
- hourly_event_volume
- country_device_breakdown
- funnel_analysis

The funnel logic required manual correction because the initial AI-generated implementation incorrectly counted users without preserving event ordering assumptions.

---

# Course Corrections

Several AI-generated suggestions were intentionally rejected to remain within assignment scope.

Examples included:
- introducing Airflow orchestration
- using distributed Spark clusters
- adding streaming infrastructure
- building dashboards and APIs
- adding unnecessary configuration layers

These were removed because the assignment explicitly emphasized simplicity and strict scope adherence.

The final implementation intentionally uses:
- local execution
- Python
- pandas
- Parquet outputs
- modular scripts only

---

# Validation Process

Every AI-generated code block was manually:
- reviewed
- tested locally
- simplified where necessary
- validated against assignment requirements

Additional checks included:
- row count validation
- duplicate verification
- malformed timestamp handling
- partition structure verification
- idempotent rerun testing

---

# What Worked Well

- Rapid scaffolding of repetitive transformation logic
- Faster iteration on schema design
- Efficient generation of aggregation patterns
- Faster README drafting

The AI significantly reduced development time for boilerplate code and documentation.

---

# Challenges Encountered

The primary challenge was controlling AI scope expansion.

The AI frequently suggested enterprise-scale solutions that exceeded assignment requirements. Maintaining a minimal and focused implementation required repeatedly reinforcing constraints.

Another challenge was validating aggregation correctness, especially for funnel calculations and session classifications.

---

# Reflection

This assignment demonstrated that effective AI-assisted development requires:
- clear constraints
- incremental prompting
- continuous validation
- deliberate simplification

The AI was treated as a collaborative assistant rather than an autonomous solution generator.

The final solution reflects a balance between:
- correctness
- simplicity
- maintainability
- assignment scope adherence