"""Scheduled / on-demand pipelines (EOD refresh, indicator recompute, etc.).

Jobs are the only place in the app allowed to drive cross-cutting writes
across providers, repositories, and the audit log. They are composed from
provider clients, repositories, and analytics helpers — they never reach
into ORM models directly.
"""
