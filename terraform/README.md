# Terraform scaffold — AWS data lake for the weather pipeline

An **Infrastructure as Code learning companion**: the AWS equivalent of
the local lake zone (`data/processed`). It is a reviewable scaffold —
CI never applies it, and no credentials or state files are committed.

## Resources

- S3 bucket (versioned, AES256 server-side encryption)
- Lifecycle rule: `raw/` objects transition to STANDARD_IA after 90 days
- Least-privilege IAM policy (GetObject / PutObject / ListBucket only)

## Usage

```bash
cd terraform
terraform init
terraform plan -var="bucket_name=my-unique-weather-lake"   # preview only
terraform apply -var="bucket_name=my-unique-weather-lake"  # requires AWS credentials
terraform destroy -var="bucket_name=..."                   # tear down
```

## Mapping to the local project

| Local (this repo) | AWS equivalent |
|---|---|
| `data/raw/*.json` | `s3://bucket/raw/` (bronze) |
| `data/processed/weather_daily/date=.../` (Parquet) | `s3://bucket/curated/weather_daily/` (silver) |
| PostgreSQL `weather_daily` / `weather_readings` | serving layer (RDS in a real deployment) |

State files (`terraform.tfstate*`, `.terraform/`, `*.tfvars`) are
gitignored — state may contain infrastructure details and must not be
committed.
