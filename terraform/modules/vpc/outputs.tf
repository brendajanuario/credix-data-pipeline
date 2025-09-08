output "vpc_network" {
  description = "The VPC network"
  value       = google_compute_network.datastream_vpc.name
}

output "vpc_network_id" {
  description = "The VPC network ID"
  value       = google_compute_network.datastream_vpc.id
}

output "vpc_subnet" {
  description = "The VPC subnet"
  value       = google_compute_subnetwork.datastream_subnet.name
}

output "vpc_subnet_id" {
  description = "The VPC subnet ID"
  value       = google_compute_subnetwork.datastream_subnet.id
}
