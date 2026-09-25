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
  description = "Name of the Confluent Cloud environment for this demo."
  type        = string
  default     = "fleet-telemetry-demo-us-west-2-v2"
}

variable "cluster_name" {
  description = "Name of the Kafka cluster."
  type        = string
  default     = "fleet-cluster-us-west-2-v2"
}

variable "cluster_cloud" {
  description = "Public cloud provider for the Kafka cluster (AWS, GCP, or AZURE)."
  type        = string
  default     = "AWS"
}

variable "cluster_region" {
  description = "Region for the Kafka cluster. us-west-2, so AI_DETECT_ANOMALIES (confirmed available in this region via rtce-bug-bash) can be tried against this pipeline's data. Sibling of the ap-southeast-2 stack in ../terraform - kept as a fully separate environment/state so this one can be torn down independently."
  type        = string
  default     = "us-west-2"
}

variable "flink_cloud" {
  description = "Cloud provider for the Flink compute pool (must match cluster_cloud - Flink runs regionally attached to the Kafka cluster's cloud/region)."
  type        = string
  default     = "AWS"
}

variable "flink_region" {
  description = "Region for the Flink compute pool. Must match cluster_region (us-west-2). Confirm this region is enabled for Flink in your org with: confluent flink region list --cloud AWS"
  type        = string
  default     = "us-west-2"
}

variable "openai_model_id" {
  description = "OpenAI model the Flink AI Advisor statements will invoke via the 'fleet-openai-connection' (flink.tf)."
  type        = string
  default     = "gpt-4o-mini"
}

variable "flink_cfu" {
  description = "Number of Confluent Flink Units (CFUs) for the compute pool. 5 is the minimum, but this pipeline runs 6 continuous streaming statements (3 business + 3 AI derivations) which exhausts 5 CFU and leaves the 6th stuck PENDING forever - 10 gives headroom."
  type        = number
  default     = 10
}

variable "openai_api_key" {
  description = "OpenAI API key used by the Flink-to-OpenAI connection (fleet-openai-connection in flink.tf)."
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
