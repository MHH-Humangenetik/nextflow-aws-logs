import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

from nextflow_aws_logs.main import (
    _make_session,
    _ms_to_utc,
    _paginate_batch,
)


def test_ms_to_utc_epoch_zero():
    """Epoch 0 ms should map to 1970-01-01 00:00:00 UTC."""
    result = _ms_to_utc(0)
    assert result == datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_ms_to_utc_known_timestamp():
    """1_000_000_000_000 ms == 2001-09-09 01:46:40 UTC."""
    result = _ms_to_utc(1_000_000_000_000)
    assert result == datetime(2001, 9, 9, 1, 46, 40, tzinfo=timezone.utc)


def test_ms_to_utc_returns_utc_aware():
    """Result must be timezone-aware and in UTC."""
    result = _ms_to_utc(1_700_000_000_000)
    assert result.tzinfo is not None
    assert result.utcoffset().total_seconds() == 0


def test_ms_to_utc_sub_second_precision():
    """Fractional seconds from ms remainder should be preserved."""
    # 1500 ms = 1 second + 500 ms → microseconds should reflect the 500 ms
    result = _ms_to_utc(1500)
    assert result.second == 1
    assert result.microsecond == 500_000


# ---------------------------------------------------------------------------
# _paginate_batch tests
# ---------------------------------------------------------------------------


def _make_batch_client(
    method: str, pages: list[tuple[list[dict], str | None]]
) -> MagicMock:
    """Build a mock boto3 Batch client for a paginated method.

    Each page is (items, nextToken). The last page should have nextToken=None.
    """
    responses = []
    for items, token in pages:
        resp: dict = {"jobSummaryList": items, "jobQueues": items}
        if token is not None:
            resp["nextToken"] = token
        responses.append(resp)

    client = MagicMock()
    getattr(client, method).side_effect = responses
    return client


def test_paginate_batch_single_page():
    """A single page with no nextToken returns all items from that page."""
    item = {"jobId": "abc", "jobName": "test"}
    client = _make_batch_client("list_jobs", [([item], None)])
    result = _paginate_batch(
        client, "list_jobs", "jobSummaryList", jobQueue="q", jobStatus="RUNNING"
    )
    assert result == [item]
    assert client.list_jobs.call_count == 1


def test_paginate_batch_multiple_pages():
    """Items from all pages are accumulated in order."""
    item1 = {"jobId": "1"}
    item2 = {"jobId": "2"}
    item3 = {"jobId": "3"}
    client = _make_batch_client(
        "list_jobs",
        [
            ([item1], "tok-1"),
            ([item2], "tok-2"),
            ([item3], None),
        ],
    )
    result = _paginate_batch(
        client, "list_jobs", "jobSummaryList", jobQueue="q", jobStatus="RUNNING"
    )
    assert result == [item1, item2, item3]
    assert client.list_jobs.call_count == 3


def test_paginate_batch_empty_result_key():
    """A page missing the result_key returns an empty list for that page."""
    client = MagicMock()
    client.list_jobs.return_value = {}  # no result_key at all
    result = _paginate_batch(
        client, "list_jobs", "jobSummaryList", jobQueue="q", jobStatus="RUNNING"
    )
    assert result == []


def test_paginate_batch_passes_kwargs():
    """Extra kwargs are forwarded to the first API call."""
    client = MagicMock()
    client.list_jobs.return_value = {"jobSummaryList": []}
    _paginate_batch(
        client, "list_jobs", "jobSummaryList", jobQueue="my-queue", jobStatus="FAILED"
    )
    client.list_jobs.assert_called_once_with(jobQueue="my-queue", jobStatus="FAILED")


def test_paginate_batch_passes_next_token_on_continuation():
    """The nextToken from page N is forwarded as nextToken= on page N+1."""
    item = {"jobId": "x"}
    client = _make_batch_client(
        "list_jobs",
        [
            ([item], "page-2-token"),
            ([], None),
        ],
    )
    _paginate_batch(
        client, "list_jobs", "jobSummaryList", jobQueue="q", jobStatus="RUNNING"
    )
    second_call_kwargs = client.list_jobs.call_args_list[1][1]
    assert second_call_kwargs["nextToken"] == "page-2-token"


# ---------------------------------------------------------------------------
# _get_all_log_events tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# _make_session tests
# ---------------------------------------------------------------------------


def _env(region="us-east-1", key_id=None, secret_key=None, profile=None):
    """Build an env-var dict for use with monkeypatching os.environ."""
    env = {"AWS_REGION": region}
    if key_id is not None:
        env["AWS_ACCESS_KEY_ID"] = key_id
    if secret_key is not None:
        env["AWS_SECRET_ACCESS_KEY"] = secret_key
    if profile is not None:
        env["AWS_PROFILE"] = profile
    return env


class TestMakeSessionRegionValidation:
    def test_missing_region_exits(self):
        """Exits with code 1 when AWS_REGION is not set."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(SystemExit) as exc:
                _make_session()
        assert exc.value.code == 1

    def test_empty_region_exits(self):
        """Exits with code 1 when AWS_REGION is an empty string."""
        with patch.dict("os.environ", {"AWS_REGION": "  "}, clear=True):
            with pytest.raises(SystemExit) as exc:
                _make_session()
        assert exc.value.code == 1


class TestMakeSessionDefaultBehavior:
    def test_default_chain_path(self):
        """Creates session with region name via default credential chain."""
        env = {"AWS_REGION": "us-west-2"}
        with patch.dict("os.environ", env, clear=True):
            with patch("boto3.Session") as mock_session_cls:
                mock_session_cls.return_value = MagicMock()
                session, region = _make_session()

        mock_session_cls.assert_called_once_with(region_name="us-west-2")
        assert region == "us-west-2"

    def test_returns_session_and_region(self):
        """Return value is a (Session, str) tuple with the correct region."""
        env = {"AWS_REGION": "ca-central-1"}
        with patch.dict("os.environ", env, clear=True):
            with patch("boto3.Session") as mock_session_cls:
                mock_instance = MagicMock()
                mock_session_cls.return_value = mock_instance
                session, region = _make_session()

        assert session is mock_instance
        assert region == "ca-central-1"


# ---------------------------------------------------------------------------
# list command unit tests  (Requirements 2.1–2.11)
# ---------------------------------------------------------------------------

from click.testing import CliRunner
from nextflow_aws_logs.main import cli


def _make_batch_session_mock(
    job_pages_by_status: dict[str, list[list[dict]]] | None = None,
):
    """Return a mock session whose batch client returns canned paginated responses.

    ``job_pages_by_status`` maps jobStatus → list-of-pages (each page is a list of job dicts).
    If a status is absent the client returns an empty first page.
    """
    job_pages_by_status = job_pages_by_status or {}

    def _list_jobs_side_effect(**kwargs):
        status = kwargs.get("jobStatus", "")
        pages = job_pages_by_status.get(status, [[]])
        # Track call count per status via a counter attached to the mock
        counter_key = f"_call_count_{status}"
        count = getattr(_list_jobs_side_effect, counter_key, 0)
        setattr(_list_jobs_side_effect, counter_key, count + 1)

        if count >= len(pages):
            return {"jobSummaryList": []}

        page = pages[count]
        resp = {"jobSummaryList": page}
        if count + 1 < len(pages):
            resp["nextToken"] = f"tok-{status}-{count + 1}"
        return resp

    batch_client = MagicMock()
    batch_client.list_jobs.side_effect = _list_jobs_side_effect

    session = MagicMock()
    session.client.return_value = batch_client
    return session, batch_client


def _runner_env():
    """Minimal env vars that pass _make_session() validation."""
    return {
        "AWS_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "TESTKEY",
        "AWS_SECRET_ACCESS_KEY": "TESTSECRET",
    }


class TestListCommandValidation:
    """Validation errors that must fire before any API call (Req 2.6, 2.11)."""

    def test_neither_from_nor_now_exits_1(self):
        """Exits with code 1 and error message when neither --from nor --now is given."""
        runner = CliRunner()
        with patch.dict("os.environ", _runner_env(), clear=True):
            with patch("boto3.Session") as mock_session_cls:
                mock_session = MagicMock()
                mock_session_cls.return_value = mock_session
                result = runner.invoke(cli, ["list", "--queue", "my-queue"])
        assert result.exit_code == 1
        assert "At least one of --from or --now must be provided" in result.output

    def test_neither_from_nor_now_makes_no_api_call(self):
        """No boto3 client is created when validation fails."""
        runner = CliRunner()
        with patch.dict("os.environ", _runner_env(), clear=True):
            with patch("boto3.Session") as mock_session_cls:
                mock_session = MagicMock()
                mock_session_cls.return_value = mock_session
                runner.invoke(cli, ["list", "--queue", "my-queue"])
        # session.client("batch") must never be called
        mock_session.client.assert_not_called()

    def test_invalid_from_date_exits_1(self):
        """Exits with code 1 and descriptive error for a non-ISO date."""
        runner = CliRunner()
        with patch.dict("os.environ", _runner_env(), clear=True):
            with patch("boto3.Session") as mock_session_cls:
                mock_session_cls.return_value = MagicMock()
                result = runner.invoke(
                    cli, ["list", "--queue", "q", "--from", "not-a-date"]
                )
        assert result.exit_code == 1
        assert "Invalid date format for --from: 'not-a-date'" in result.output
        assert "ISO 8601" in result.output

    def test_invalid_from_date_makes_no_api_call(self):
        """No API call is made when --from is an invalid date string."""
        runner = CliRunner()
        with patch.dict("os.environ", _runner_env(), clear=True):
            with patch("boto3.Session") as mock_session_cls:
                mock_session = MagicMock()
                mock_session_cls.return_value = mock_session
                runner.invoke(cli, ["list", "--queue", "q", "--from", "bad-date"])
        mock_session.client.assert_not_called()

    def test_empty_queue_string_exits_1(self):
        """Exits with code 1 when --queue is an empty string."""
        runner = CliRunner()
        with patch.dict("os.environ", _runner_env(), clear=True):
            with patch("boto3.Session") as mock_session_cls:
                mock_session_cls.return_value = MagicMock()
                result = runner.invoke(cli, ["list", "--queue", "", "--now"])
        assert result.exit_code == 1
        assert "cannot be an empty string" in result.output

    def test_empty_queue_string_makes_no_api_call(self):
        """No API call is made when --queue is empty."""
        runner = CliRunner()
        with patch.dict("os.environ", _runner_env(), clear=True):
            with patch("boto3.Session") as mock_session_cls:
                mock_session = MagicMock()
                mock_session_cls.return_value = mock_session
                runner.invoke(cli, ["list", "--queue", "", "--now"])
        mock_session.client.assert_not_called()


class TestListCommandResults:
    """Successful list command scenarios (Req 2.1–2.10)."""

    def _invoke_list(self, args, job_pages_by_status=None):
        """Helper to invoke `list` with a canned batch mock."""
        session, batch_client = _make_batch_session_mock(job_pages_by_status)
        runner = CliRunner()
        with patch.dict("os.environ", _runner_env(), clear=True):
            with patch("boto3.Session", return_value=session):
                result = runner.invoke(cli, ["list"] + args)
        return result, batch_client

    def test_now_flag_queries_running_only(self):
        """--now causes only the RUNNING status to be queried (Req 2.4)."""
        result, batch_client = self._invoke_list(
            ["--queue", "q", "--now"],
            {
                "RUNNING": [
                    [
                        {
                            "jobName": "j",
                            "jobId": "id1",
                            "status": "RUNNING",
                            "createdAt": 1_000_000_000_000,
                        }
                    ]
                ]
            },
        )
        assert result.exit_code == 0
        # list_jobs should only have been called with jobStatus="RUNNING"
        calls = batch_client.list_jobs.call_args_list
        statuses_queried = {c.kwargs.get("jobStatus") for c in calls}
        assert statuses_queried == {"RUNNING"}

    def test_from_only_queries_all_statuses(self):
        """--from only causes all 7 statuses to be queried (Req 2.3)."""
        from nextflow_aws_logs.main import BATCH_STATUSES

        result, batch_client = self._invoke_list(
            ["--queue", "q", "--from", "2020-01-01"]
        )
        calls = batch_client.list_jobs.call_args_list
        statuses_queried = {c.kwargs.get("jobStatus") for c in calls}
        assert statuses_queried == set(BATCH_STATUSES)

    def test_both_flags_queries_running_only(self):
        """--from + --now causes only RUNNING to be queried (Req 2.5)."""
        result, batch_client = self._invoke_list(
            ["--queue", "q", "--from", "2020-01-01", "--now"]
        )
        calls = batch_client.list_jobs.call_args_list
        statuses_queried = {c.kwargs.get("jobStatus") for c in calls}
        assert statuses_queried == {"RUNNING"}

    def test_empty_result_shows_table_headers_exit_0(self):
        """Empty result set shows table with no data rows, exits 0 (Req 2.8)."""
        result, _ = self._invoke_list(["--queue", "q", "--now"])
        assert result.exit_code == 0
        assert "Job name" in result.output
        assert "Status" in result.output
        assert "Created at" in result.output

    def test_non_empty_result_shows_job_details(self):
        """Job name, ID, status, and formatted timestamp appear in the table (Req 2.7)."""
        ts_ms = 1_000_000_000_000  # 2001-09-09T01:46:40+00:00
        job = {
            "jobName": "my-job",
            "jobId": "abc-123",
            "status": "RUNNING",
            "createdAt": ts_ms,
        }
        result, _ = self._invoke_list(
            ["--queue", "q", "--now"],
            {"RUNNING": [[job]]},
        )
        assert result.exit_code == 0
        assert "my-job" in result.output
        assert "RUNNING" in result.output
        assert "2001-09-09" in result.output

    def test_from_filter_excludes_old_jobs(self):
        """Jobs created before --from date are excluded (Req 2.3)."""
        old_ts = 1_000_000_000_000  # 2001-09-09 — before filter
        new_ts = 1_700_000_000_000  # 2023-11-14 — after filter
        jobs = [
            {
                "jobName": "old-job",
                "jobId": "id-old",
                "status": "RUNNING",
                "createdAt": old_ts,
            },
            {
                "jobName": "new-job",
                "jobId": "id-new",
                "status": "RUNNING",
                "createdAt": new_ts,
            },
        ]
        result, _ = self._invoke_list(
            ["--queue", "q", "--from", "2023-01-01"],
            {
                "RUNNING": [jobs],
                "SUBMITTED": [[]],
                "PENDING": [[]],
                "RUNNABLE": [[]],
                "STARTING": [[]],
                "SUCCEEDED": [[]],
                "FAILED": [[]],
            },
        )
        assert result.exit_code == 0
        assert "new-job" in result.output
        assert "old-job" not in result.output

    def test_from_filter_inclusive_lower_bound(self):
        """A job created exactly at the --from date boundary is included (Req 2.3)."""
        # 2023-01-01T00:00:00 UTC in milliseconds
        boundary_ts = int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        job = {
            "jobName": "boundary-job",
            "jobId": "id-b",
            "status": "SUCCEEDED",
            "createdAt": boundary_ts,
        }
        result, _ = self._invoke_list(
            ["--queue", "q", "--from", "2023-01-01"],
            {
                "SUCCEEDED": [[job]],
                "SUBMITTED": [[]],
                "PENDING": [[]],
                "RUNNABLE": [[]],
                "STARTING": [[]],
                "RUNNING": [[]],
                "FAILED": [[]],
            },
        )
        assert result.exit_code == 0
        assert "boundary-job" in result.output

    def test_valid_iso_date_is_accepted(self):
        """A well-formed ISO 8601 date passes validation and proceeds to API (Req 2.11)."""
        result, batch_client = self._invoke_list(
            ["--queue", "q", "--from", "2024-06-15"]
        )
        # Should not exit early with validation error
        assert "Invalid date format" not in result.output
        assert batch_client.list_jobs.called


class TestListCommandErrors:
    """Error handling: queue not found, generic API error (Req 2.9, 2.10)."""

    def _make_client_error(self, code: str, message: str):
        error_response = {"Error": {"Code": code, "Message": message}}
        return botocore.exceptions.ClientError(error_response, "ListJobs")

    def _invoke_list_with_error(self, error):
        batch_client = MagicMock()
        batch_client.list_jobs.side_effect = error
        session = MagicMock()
        session.client.return_value = batch_client
        runner = CliRunner()
        with patch.dict("os.environ", _runner_env(), clear=True):
            with patch("boto3.Session", return_value=session):
                return runner.invoke(cli, ["list", "--queue", "no-such-queue", "--now"])

    def test_queue_not_found_exits_1(self):
        """InvalidJobQueueException maps to a helpful message and exit code 1 (Req 2.9)."""
        err = self._make_client_error(
            "InvalidJobQueueException", "Queue does not exist"
        )
        result = self._invoke_list_with_error(err)
        assert result.exit_code == 1
        assert "does not exist" in result.output
        assert "no-such-queue" in result.output

    def test_generic_api_error_exits_1(self):
        """Generic ClientError surfaces the AWS message and exits 1 (Req 2.10)."""
        err = self._make_client_error("ThrottlingException", "Rate exceeded")
        result = self._invoke_list_with_error(err)
        assert result.exit_code == 1
        assert "AWS Batch API error" in result.output
        assert "Rate exceeded" in result.output
