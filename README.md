# nextflow-aws-logs

A CLI for inspecting AWS Batch jobs and their CloudWatch logs without touching the console.

## Commands

**`list`** — Show jobs in a queue, filtered by date and/or status.

```
nextflow-aws-logs list --queue my-queue --from 2024-01-01
nextflow-aws-logs list --queue my-queue --now
nextflow-aws-logs list --queue my-queue --from 2024-01-01 --now
```

**`show-log`** — Stream CloudWatch logs for the most recent run of a named job.

```
nextflow-aws-logs show-log --job-name my-job --queue my-queue
```

**`list-queues`** — List all job queues and how many jobs are currently running in each.

```
nextflow-aws-logs list-queues
```

## Configuration

The tool reads configuration from environment variables (or a `.env` file in the working directory).

| Variable | Required | Description |
|---|---|---|
| `AWS_REGION` | Yes | AWS region to target |
| `AWS_ACCESS_KEY_ID` | No | Explicit access key (must be paired with secret) |
| `AWS_SECRET_ACCESS_KEY` | No | Explicit secret key (must be paired with key ID) |
| `AWS_PROFILE` | No | Named profile from `~/.aws/credentials` |

If neither explicit keys nor a profile are set, the tool falls back to the default boto3 credential chain (instance profile, ECS task role, etc.).

## IAM permissions required

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "batch:ListJobs",
        "batch:DescribeJobs",
        "batch:DescribeJobQueues"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:GetLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/batch/job:*"
    }
  ]
}
```
