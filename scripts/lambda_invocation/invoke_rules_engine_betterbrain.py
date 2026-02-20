import boto3
import json
import argparse

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", type=str, required=False)
    args = parser.parse_args()

    session_params = {}

    profile = args.profile
    if profile:
        session_params["profile_name"] = profile

    session = boto3.Session(**session_params)
    lambda_client = session.client("lambda")

    response = lambda_client.invoke(
        FunctionName='rules-engine-betterbrain',
        InvocationType='RequestResponse',
        Payload=json.dumps({
            'organization_id': 'catholic-charities'
        })
    )

    print(f"Response: {response}")

    # result = json.loads(response['Payload'].read())
    # print(f"Validation run ID: {result['validation_run_id']}")
    # print(f"Files processed: {result['files_processed']}")

if __name__ == "__main__":
    main()

# python -m scripts.lambda_invocation.invoke_rules_engine_betterbrain --profile penguin