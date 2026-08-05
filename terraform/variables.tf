variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (organization-level, used to create resources). Get this from https://confluent.cloud -> top-right menu -> API Keys -> Cloud API Key."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret matching confluent_cloud_api_key."
  type        = string
  sensitive   = true
}

variable "environment_name" {
  description = "Name of the Confluent Cloud environment for this hackathon demo."
  type        = string
  default     = "fleet-telemetry-hackathon"
}

variable "cluster_name" {
  description = "Name of the Kafka cluster."
  type        = string
  default     = "fleet-cluster"
}

variable "cluster_cloud" {
  description = "Public cloud provider for the Kafka cluster (AWS, GCP, or AZURE). AWS Sydney, since this demo calls Bedrock."
  type        = string
  default     = "AWS"
}

variable "cluster_region" {
  description = "Region for the Kafka cluster. ap-southeast-2 = Sydney, kept in-region for data residency and to sit next to Bedrock."
  type        = string
  default     = "ap-southeast-2"
}

variable "flink_cloud" {
  description = "Cloud provider for the Flink compute pool (must match cluster_cloud - Flink runs regionally attached to the Kafka cluster's cloud/region)."
  type        = string
  default     = "AWS"
}

variable "flink_region" {
  description = "Region for the Flink compute pool. Must match cluster_region (ap-southeast-2 / Sydney). Confirm this region is enabled for Flink in your org with: confluent flink region list --cloud AWS"
  type        = string
  default     = "ap-southeast-2"
}

variable "bedrock_region" {
  description = "AWS region for the Bedrock endpoint Flink calls. Bedrock is not available in every region - ap-southeast-2 (Sydney) does host Bedrock, but confirm current model availability there, since not all foundation models are offered in every Bedrock region. Fall back to a region like us-east-1 if the model you need isn't in Sydney yet."
  type        = string
  default     = "ap-southeast-2"
}

variable "bedrock_model_id" {
  description = "Bedrock model ID the Flink AI Advisor statements will invoke. Uses the 'global.' cross-region inference profile prefix - most current Claude models on Bedrock reject bare on-demand model IDs entirely ('on-demand throughput isn't supported'), and this account's regional profiles ('us.', 'apac.') weren't recognized for this model, only 'global.' - verified working directly via boto3 invoke_model."
  type        = string
  default     = "global.anthropic.claude-haiku-4-5-20251001-v1:0"
}

variable "flink_cfu" {
  description = "Number of Confluent Flink Units (CFUs) for the compute pool. 5 is the minimum, but this pipeline runs 6 continuous streaming statements (3 business + 3 AI derivations) which exhausts 5 CFU and leaves the 6th stuck PENDING forever - 10 gives headroom."
  type        = number
  default     = 10
}

variable "bedrock_aws_access_key_id" {
  description = "AWS access key ID for an IAM user/role scoped to bedrock:InvokeModel only, used by the Flink-to-Bedrock connection. Do not reuse a broad admin key here."
  type        = string
  sensitive   = true
}

variable "bedrock_aws_secret_access_key" {
  description = "AWS secret access key matching bedrock_aws_access_key_id."
  type        = string
  sensitive   = true
}

variable "vehicle_topic_partitions" {
  description = "Partition count for the raw vehicle.telemetry topic (higher throughput topic)."
  type        = number
  default     = 6
}

variable "derived_topic_partitions" {
  description = "Partition count for derived/business/AI topics (lower volume)."
  type        = number
  default     = 3
}
