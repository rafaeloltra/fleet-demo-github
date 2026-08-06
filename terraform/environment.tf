# A dedicated environment keeps everything for this demo isolated
# and easy to tear down with `terraform destroy`.
resource "confluent_environment" "fleet" {
  display_name = var.environment_name

  stream_governance {
    package = "ADVANCED" # upgraded from ESSENTIALS outside Terraform at some point during this session -
    # do not revert to ESSENTIALS via Terraform: doing so replaces the Schema Registry cluster
    # (new ID), which would orphan every schema registered for the 10 topics in flink_statements.tf
  }
}

# Standard cluster: single zone, pay-as-you-go. Needed (not Basic) because
# fine-grained RBAC resource roles scoped to individual topics/groups -
# used throughout topics.tf and flink.tf - are rejected on Basic clusters
# ("Basic Clusters can not use resource roles").
resource "confluent_kafka_cluster" "fleet" {
  display_name = var.cluster_name
  availability = "SINGLE_ZONE"
  cloud        = var.cluster_cloud
  region       = var.cluster_region

  standard {}

  environment {
    id = confluent_environment.fleet.id
  }
}
