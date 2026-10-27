output "bucket_name" {
  description = "Name of the created data lake bucket."
  value       = aws_s3_bucket.weather_lake.id
}

output "bucket_arn" {
  description = "ARN of the data lake bucket."
  value       = aws_s3_bucket.weather_lake.arn
}

output "pipeline_policy_arn" {
  description = "ARN of the least-privilege pipeline IAM policy."
  value       = aws_iam_policy.pipeline_rw.arn
}
