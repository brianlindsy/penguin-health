import boto3


cloudwatch = boto3.client('cloudwatch')

NAMESPACE = 'PenguinHealth/FhirMaterializer'


def emit(metric_name, org_id, *, value=1, reason=None, unit='Count'):
    dimensions = [{'Name': 'OrganizationId', 'Value': org_id}]
    if reason:
        dimensions.append({'Name': 'Reason', 'Value': reason})
    try:
        cloudwatch.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Dimensions': dimensions,
            }],
        )
    except Exception as e:
        # Never fail an invocation due to metrics. Log and move on.
        print(f"metric emit failed: {metric_name} org={org_id} reason={reason} err={e}")
