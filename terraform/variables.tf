variable "aws_region" {
  description = "AWS region for the data lake resources."
  type        = string
  default     = "eu-central-1"
}

variable "bucket_name" {
  description = "Globally unique name of the S3 data lake bucket."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name must be a valid S3 bucket name (3-63 chars, lowercase)."
  }
}
