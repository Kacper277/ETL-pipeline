# ---------------------------------------------------------------------------
# Weather data lake - AWS scaffold (IaC learning companion to the repo)
#
# PURPOSE: demonstrate Infrastructure as Code basics for the trainee
# portfolio. This scaffold is NOT applied by CI and holds no secrets.
# The equivalent of the local `data/processed` lake zone on AWS is a
# versioned S3 bucket with server-side encryption and a lifecycle rule.
#
# Usage (requires AWS credentials in the environment or ~/.aws):
#   terraform init
#   terraform plan  -var="bucket_name=my-weather-lake"
#   terraform apply -var="bucket_name=my-weather-lake"   # NOT done by CI
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Curated zone of the lake: partitioned Parquet output of the batch
# pipeline lands here (data/processed/weather_daily/date=YYYY-MM-DD/).
resource "aws_s3_bucket" "weather_lake" {
  bucket = var.bucket_name

  tags = {
    Project     = "weather-pipeline"
    Environment = "demo"
    Zone        = "curated"
  }
}

resource "aws_s3_bucket_versioning" "weather_lake" {
  bucket = aws_s3_bucket.weather_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "weather_lake" {
  bucket = aws_s3_bucket.weather_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Raw JSON snapshots are cold after ~90 days: tier them down.
resource "aws_s3_bucket_lifecycle_configuration" "weather_lake" {
  bucket = aws_s3_bucket.weather_lake.id

  rule {
    id     = "tier-raw-json-to-ia"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}

# Least-privilege read/write for the pipeline (no admin access).
data "aws_iam_policy_document" "pipeline_rw" {
  statement {
    sid       = "WeatherLakeObjectAccess"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.weather_lake.arn,
      "${aws_s3_bucket.weather_lake.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "pipeline_rw" {
  name        = "${var.bucket_name}-pipeline-rw"
  description = "Read/write access to the weather data lake bucket"
  policy      = data.aws_iam_policy_document.pipeline_rw.json
}
