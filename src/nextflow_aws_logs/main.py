import os
import sys
from datetime import datetime, timezone
from typing import Any

import boto3
import botocore.exceptions
import click
from dotenv import load_dotenv
from rich.table import Table
from rich.console import Console

console = Console()

BATCH_STATUSES = [
    "SUBMITTED",
    "PENDING",
    "RUNNABLE",
    "STARTING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
]


def _ms_to_utc(ms: int) -> datetime:
    """Convert a millisecond epoch integer to a UTC-aware datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _paginate_batch(
    client, method: str, result_key: str, **kwargs
) -> list[dict[str, Any]]:
    """Exhaust nextToken pagination for a boto3 Batch method.

    Returns a flat list of all items from ``result_key`` across all pages.
    """
    items: list[dict[str, Any]] = []
    response = getattr(client, method)(**kwargs)
    items.extend(response.get(result_key, []))

    while "nextToken" in response:
        response = getattr(client, method)(nextToken=response["nextToken"], **kwargs)
        items.extend(response.get(result_key, []))

    return items


def _make_session() -> tuple[boto3.Session, str]:
    region = os.environ.get("AWS_REGION", "").strip()
    if not region:
        console.print(
            "[bold red]Error:[/bold red] AWS_REGION environment variable is not set"
        )
        sys.exit(1)
    return boto3.Session(region_name=region), region


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    """nextflow-aws-logs — inspect AWS Batch jobs and CloudWatch logs.

    \b
    Example:
        nextflow-aws-logs list --queue my-queue --from 2024-01-01
    """
    load_dotenv()
    session, region = _make_session()
    ctx.ensure_object(dict)
    ctx.obj["session"] = session
    ctx.obj["region"] = region


@cli.command("list")
@click.option("--queue", required=True, help="Job queue name.")
@click.option(
    "--from", "from_date", default=None, help="ISO 8601 date lower bound (UTC)."
)
@click.option(
    "--now", is_flag=True, default=False, help="Restrict to RUNNING jobs only."
)
@click.pass_context
def list_jobs(ctx: click.Context, queue: str, from_date: str | None, now: bool) -> None:
    """List jobs in a queue filtered by date and/or status.

    \b
    Example:
        nextflow-aws-logs list --queue my-queue --from 2024-01-01
        nextflow-aws-logs list --queue my-queue --now
    """
    if not from_date and not now:
        console.print(
            "[bold red]Error:[/bold red] At least one of --from or --now must be provided"
        )
        sys.exit(1)

    if queue == "":
        console.print("[bold red]Error:[/bold red] --queue cannot be an empty string")
        sys.exit(1)

    # Validate --from date before any API call
    from_dt: datetime | None = None
    if from_date:
        try:
            from_dt = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
        except ValueError:
            console.print(
                f"[bold red]Error:[/bold red] "
                f"Invalid date format for --from: '{from_date}'. Expected ISO 8601 (e.g. 2024-01-15)"
            )
            sys.exit(1)

    with console.status(f"Getting jobs for [italic]{queue}[/italic]..."):
        session: boto3.Session = ctx.obj["session"]
        batch = session.client("batch")

        # Determine which statuses to query
        if now:
            statuses = ["RUNNING"]
        else:
            statuses = BATCH_STATUSES

        try:
            jobs: list[dict[str, Any]] = []
            for status in statuses:
                jobs.extend(
                    _paginate_batch(
                        batch,
                        "list_jobs",
                        "jobSummaryList",
                        jobQueue=queue,
                        jobStatus=status,
                    )
                )
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("InvalidJobQueueException",):
                console.print(
                    f"[bold red]Error:[/bold red] Job queue '{queue}' does not exist"
                )
            else:
                console.print(
                    f"[bold red]Error:[/bold red] AWS Batch API error: {e.response['Error']['Message']}"
                )
            sys.exit(1)

        # Apply date filter client-side
        if from_dt is not None:
            jobs = [j for j in jobs if _ms_to_utc(j["createdAt"]) >= from_dt]

        table = Table(show_header=True, header_style="bold")
        table.add_column("Job name")
        table.add_column("Created at")
        table.add_column("Status")

        for job in jobs:
            table.add_row(
                job["jobName"],
                _ms_to_utc(job["createdAt"]).isoformat(),
                job["status"],
            )

    console.print(table)


@cli.command("show-log")
@click.option("--job-name", required=True, help="Exact job name to look up.")
@click.option("--queue", required=True, help="Job queue name.")
@click.pass_context
def show_log(ctx: click.Context, job_name: str, queue: str) -> None:
    """Show CloudWatch logs for the most recent job matching --job-name.

    \b
    Example:
        nextflow-aws-logs show-log --job-name my-job --queue my-queue
    """
    if job_name == "":
        console.print(
            "[bold red]Error:[/bold red] --job-name cannot be an empty string"
        )
        sys.exit(1)
    if queue == "":
        console.print("[bold red]Error:[/bold red] --queue cannot be an empty string")
        sys.exit(1)

    with console.status("Getting log..."):

        session: boto3.Session = ctx.obj["session"]
        batch = session.client("batch")

        try:
            jobs: list[dict[str, Any]] = []
            for status in BATCH_STATUSES:
                jobs.extend(
                    _paginate_batch(
                        batch,
                        "list_jobs",
                        "jobSummaryList",
                        jobQueue=queue,
                        jobStatus=status,
                    )
                )
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("InvalidJobQueueException",):
                console.print(
                    f"[bold red]Error:[/bold red] Job queue '{queue}' does not exist"
                )
            else:
                console.print(
                    f"[bold red]Error:[/bold red] AWS Batch API error: {e.response['Error']['Message']}"
                )
            sys.exit(1)

        matching = [j for j in jobs if j["jobName"] == job_name]
        if not matching:
            console.print(
                f"[bold red]Error:[/bold red] No job named '{job_name}' found in queue '{queue}'"
            )
            sys.exit(1)

        # Most recent first
        most_recent = max(matching, key=lambda j: j["createdAt"])

        # Describe job to get log stream
        try:
            describe_resp = batch.describe_jobs(jobs=[most_recent["jobId"]])
        except botocore.exceptions.ClientError as e:
            console.print(
                f"[bold red]Error:[/bold red] AWS Batch API error: {e.response['Error']['Message']}"
            )
            sys.exit(1)

        job_detail = describe_resp["jobs"][0]
        log_stream = job_detail.get("container", {}).get("logStreamName")

        if not log_stream:
            console.print(
                f"No log stream available for this job (status: {job_detail.get('status', 'UNKNOWN')})"
            )
            sys.exit(0)

        logs_client = session.client("logs")
        log_group = "/aws/batch/job"

        try:
            events: list[dict[str, Any]] = []
            response = logs_client.get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream,
                startFromHead=True,
            )
            events.extend(response.get("events", []))
            prev_token = response.get("nextForwardToken")
            while True:
                response = logs_client.get_log_events(
                    logGroupName=log_group,
                    logStreamName=log_stream,
                    startFromHead=True,
                    nextToken=prev_token,
                )
                next_token = response.get("nextForwardToken")
                events.extend(response.get("events", []))
                if next_token == prev_token:
                    break
                prev_token = next_token
        except botocore.exceptions.ClientError as e:
            console.print(
                f"[bold red]Error:[/bold red] CloudWatch Logs API error: {e.response['Error']['Message']} (log group: {log_group})"
            )
            sys.exit(1)

    for event in events:
        ts = _ms_to_utc(event["timestamp"]).isoformat()
        console.print(f"{ts} - {event['message']}")


@cli.command("list-queues")
@click.pass_context
def list_queues(ctx: click.Context) -> None:
    """List all job queues and their current running job counts.

    \b
    Example:
        nextflow-aws-logs list-queues

    """
    
    with console.status("Getting queues..."):
        session: boto3.Session = ctx.obj["session"]
        batch = session.client("batch")

        try:
            queues = _paginate_batch(batch, "describe_job_queues", "jobQueues")
        except botocore.exceptions.ClientError as e:
            console.print(
                f"[bold red]Error:[/bold red] AWS Batch API error: {e.response['Error']['Message']}"
            )
            sys.exit(1)

        if not queues:
            console.print("No job queues found in this account and region")
            sys.exit(0)

        table = Table(show_header=True, header_style="bold")
        table.add_column("Queue Name")
        table.add_column("State")
        table.add_column("Running Jobs")

        for queue in queues:
            name = queue["jobQueueName"]
            state = queue.get("state", "UNKNOWN")
            try:
                running_jobs = _paginate_batch(
                    batch, "list_jobs", "jobSummaryList", jobQueue=name, jobStatus="RUNNING"
                )
                running_count = str(len(running_jobs))
            except botocore.exceptions.ClientError:
                running_count = "N/A"

            table.add_row(name, state, running_count)

    console.print(table)


if __name__ == "__main__":
    cli()
