# S3 Setup for Icechunk Store

This document covers setting up an S3 bucket to host the icechunk store, configuring permissions, and managing access from local development and GitHub Actions.

## 1. Create the S3 bucket

```bash
aws s3api create-bucket \
  --bucket opr-radar-metrics \
  --region us-west-2 \
  --create-bucket-configuration LocationConstraint=us-west-2
```

Enable versioning (recommended for data safety, not strictly required since icechunk handles its own versioning):

```bash
aws s3api put-bucket-versioning \
  --bucket opr-radar-metrics \
  --versioning-configuration Status=Enabled
```

## 2. Bucket policy

The bucket needs two access patterns:
- **Read-write** for the pipeline (local runs and CI)
- **Read-only public** for the icechunk-js browser visualization (optional)

### Private read-write (no public access)

If you don't need public read access, no bucket policy is needed — IAM policies on the user/role handle everything.

### Public read access for icechunk-js

To allow the browser-based visualization to read the store without credentials, add a bucket policy for public read:

First, you'll also need to disable the S3 Block Public Access settings:

```bash
aws s3api put-public-access-block \
  --bucket opr-radar-metrics \
  --public-access-block-configuration \
    BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false
```

Then create the policy as a JSON file:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicReadIcechunk",
      "Effect": "Allow",
      "Principal": "*",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::opr-radar-metrics",
        "arn:aws:s3:::opr-radar-metrics/*"
      ]
    }
  ]
}
```

Apply it:

```bash
aws s3api put-bucket-policy \
  --bucket opr-radar-metrics \
  --policy file://bucket-policy.json
```

### CORS (required for icechunk-js browser access)

```json
{
  "CORSRules": [
    {
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "HEAD"],
      "AllowedHeaders": ["*"],
      "MaxAgeSeconds": 3600
    }
  ]
}
```

```bash
aws s3api put-bucket-cors \
  --bucket opr-radar-metrics \
  --cors-configuration file://cors-config.json
```

## 3. IAM setup

### IAM policy for pipeline read-write access

Create a policy that grants read-write access to the bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "IcechunkReadWrite",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::opr-radar-metrics",
        "arn:aws:s3:::opr-radar-metrics/*"
      ]
    }
  ]
}
```

```bash
aws iam create-policy \
  --policy-name OPRIcechunkReadWrite \
  --policy-document file://iam-policy.json
```

## 4. Local development access

### Option A: IAM user with access keys (simplest)

Create an IAM user and attach the policy:

```bash
aws iam create-user --user-name opr-radar-dev
aws iam attach-user-policy \
  --user-name opr-radar-dev \
  --policy-arn arn:aws:iam::ACCOUNT_ID:policy/OPRIcechunkReadWrite
aws iam create-access-key --user-name opr-radar-dev
```

Configure locally:

```bash
aws configure --profile opr-metrics
# Enter the access key ID and secret from the previous command
```

Then either:
- Set `AWS_PROFILE=opr-metrics` before running the pipeline, or
- Export the credentials directly:
  ```bash
  export AWS_ACCESS_KEY_ID=...
  export AWS_SECRET_ACCESS_KEY=...
  export AWS_DEFAULT_REGION=us-west-2
  ```

The icechunk `from_env=True` setting picks up credentials from the standard AWS credential chain: environment variables, `~/.aws/credentials`, IAM instance role, etc.

### Option B: AWS SSO (recommended for organizations)

If your AWS account uses SSO/Identity Center:

```bash
aws configure sso --profile opr-metrics
# Follow the prompts to set up SSO
aws sso login --profile opr-metrics
export AWS_PROFILE=opr-metrics
```

## 5. GitHub Actions access

### Recommended: OIDC federation (no stored secrets)

OIDC lets GitHub Actions assume an IAM role directly without storing long-lived credentials. This is the recommended approach.

#### Step 1: Create an OIDC identity provider in AWS

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

(The thumbprint may change — check [GitHub's docs](https://docs.github.com/en/actions/security-for-github-actions/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services) for the current value.)

#### Step 2: Create an IAM role for GitHub Actions

Trust policy (`github-actions-trust.json`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/radar-return-statistics:*"
        }
      }
    }
  ]
}
```

Replace `ACCOUNT_ID` with your AWS account ID and `YOUR_ORG/radar-return-statistics` with the actual repo path. You can restrict the `sub` condition further, e.g. to a specific branch:

```
"repo:YOUR_ORG/radar-return-statistics:ref:refs/heads/main"
```

Create the role:

```bash
aws iam create-role \
  --role-name OPRRadarGitHubActions \
  --assume-role-policy-document file://github-actions-trust.json

aws iam attach-role-policy \
  --role-name OPRRadarGitHubActions \
  --policy-arn arn:aws:iam::ACCOUNT_ID:policy/OPRIcechunkReadWrite
```

#### Step 3: GitHub Actions workflow

```yaml
name: Process radar data

on:
  workflow_dispatch:
  schedule:
    - cron: '0 6 * * 1'  # Weekly Monday 6am UTC

permissions:
  id-token: write   # Required for OIDC
  contents: read

jobs:
  process:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::ACCOUNT_ID:role/OPRRadarGitHubActions
          aws-region: us-west-2

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Run pipeline
        run: uv run python -m radar_return_statistics config/config.yaml
```

The `aws-actions/configure-aws-credentials` action sets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` as environment variables, which icechunk picks up via `from_env=True`.

### Alternative: Repository secrets (simpler but less secure)

If OIDC is too complex for your setup, store credentials as GitHub repository secrets:

1. Go to repository Settings > Secrets and variables > Actions
2. Add `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`

```yaml
      - name: Configure AWS credentials
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          AWS_DEFAULT_REGION: us-west-2
```

This works but means long-lived credentials are stored in GitHub. Prefer OIDC when possible.

## 6. Pipeline configuration

Update `config/config.yaml`:

```yaml
store:
  backend: "s3"
  s3_bucket: "opr-radar-metrics"
  s3_prefix: "icechunk/david-drygalski"
  s3_region: "us-west-2"
```

For local development with a local store (no AWS needed):

```yaml
store:
  backend: "local"
  path: "outputs/icechunk_store"
```

## 7. Cost considerations

Icechunk on S3 uses standard S3 pricing:
- **Storage**: ~$0.023/GB/month (S3 Standard)
- **Requests**: $0.005 per 1,000 PUT/POST, $0.0004 per 1,000 GET
- **Data transfer**: Free within same region, $0.09/GB out to internet

For this dataset (~104 frames, ~57K traces), storage costs will be negligible. The main cost driver would be frequent GET requests from the browser visualization if it gets significant traffic. Consider CloudFront caching in front of the bucket if that becomes an issue.
