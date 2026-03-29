#!/bin/bash
# =============================================================================
# GCP e2-micro VM Setup Script
# =============================================================================
# Run this on your local machine to create and configure the GCP VM.
# Prerequisites: gcloud CLI installed and authenticated.
#
# Usage:
#   chmod +x deploy/gcp-setup.sh
#   ./deploy/gcp-setup.sh
# =============================================================================

set -e

PROJECT_ID=$(gcloud config get-value project)
ZONE="us-central1-a"
VM_NAME="risk-mgmt-vm"

echo "=== Creating GCE e2-micro VM (free tier) ==="
gcloud compute instances create $VM_NAME \
    --zone=$ZONE \
    --machine-type=e2-micro \
    --image-family=cos-stable \
    --image-project=cos-cloud \
    --boot-disk-size=30GB \
    --tags=kafka-server \
    --metadata=startup-script='#!/bin/bash
# Install Docker Compose
mkdir -p /opt/risk-mgmt
cd /opt/risk-mgmt
'

echo ""
echo "=== Opening firewall for Kafka (internal only) ==="
gcloud compute firewall-rules create allow-kafka-internal \
    --allow=tcp:9092 \
    --source-ranges=10.0.0.0/8 \
    --target-tags=kafka-server \
    --description="Allow internal Kafka traffic" \
    2>/dev/null || echo "Firewall rule already exists"

echo ""
echo "=== VM created! ==="
echo ""
echo "Next steps:"
echo "  1. Copy your project files to the VM:"
echo "     gcloud compute scp --recurse . $VM_NAME:/opt/risk-mgmt --zone=$ZONE"
echo ""
echo "  2. Create .env file on the VM:"
echo "     gcloud compute ssh $VM_NAME --zone=$ZONE --command='cat > /opt/risk-mgmt/.env << EOF"
echo "SNOWFLAKE_ACCOUNT=your_account_here"
echo "SNOWFLAKE_USER=your_user_here"
echo "SNOWFLAKE_PASSWORD=your_password_here"
echo "KAFKA_HOST=\$(hostname -I | awk \"{print \\\$1}\")"
echo "EOF'"
echo ""
echo "  3. SSH in and start services:"
echo "     gcloud compute ssh $VM_NAME --zone=$ZONE"
echo "     cd /opt/risk-mgmt"
echo "     docker compose up -d"
echo ""
echo "  4. Get the VM's internal IP for Streamlit Cloud config:"
echo "     gcloud compute instances describe $VM_NAME --zone=$ZONE --format='get(networkInterfaces[0].accessConfigs[0].natIP)'"
