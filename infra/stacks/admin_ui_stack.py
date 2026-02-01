from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    BundlingOptions,
    ILocalBundling,
    aws_cognito as cognito,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_apigatewayv2 as apigwv2,
)
from aws_cdk.aws_apigatewayv2_integrations import HttpLambdaIntegration
from aws_cdk.aws_apigatewayv2_authorizers import HttpJwtAuthorizer
from constructs import Construct
import jsii
import os
import shutil


@jsii.implements(ILocalBundling)
class CopyFileBundler:
    """Bundles a single file into the Lambda asset output directory."""

    def __init__(self, source_path: str):
        self._source_path = source_path

    def try_bundle(self, output_dir: str, options) -> bool:
        shutil.copy2(self._source_path, output_dir)
        return True


class AdminUiStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ----- Cognito -----
        user_pool = cognito.UserPool(self, "AdminUserPool",
            user_pool_name="penguin-health-admin-pool",
            self_sign_up_enabled=False,
            sign_in_aliases=cognito.SignInAliases(email=True),
            password_policy=cognito.PasswordPolicy(
                min_length=12,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=True,
            ),
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            removal_policy=RemovalPolicy.RETAIN,
        )

        app_client = user_pool.add_client("AdminAppClient",
            user_pool_client_name="penguin-health-admin-app",
            auth_flows=cognito.AuthFlow(
                user_srp=True,
            ),
            id_token_validity=Duration.hours(1),
            access_token_validity=Duration.hours(1),
            refresh_token_validity=Duration.days(30),
        )

        cognito.CfnUserPoolGroup(self, "AdminsGroup",
            user_pool_id=user_pool.user_pool_id,
            group_name="Admins",
            description="Admin users with full access to organization configuration",
        )

        # ----- Reference existing DynamoDB table -----
        org_config_table = dynamodb.Table.from_table_name(
            self, "OrgConfigTable", "penguin-health-org-config"
        )

        # ----- Admin API Lambda -----
        admin_api_fn = _lambda.Function(self, "AdminApiFunction",
            function_name="penguin-health-admin-api",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="admin_api.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "..", "..", "lambda"),
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=CopyFileBundler(
                        os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "admin_api.py")
                    ),
                ),
            ),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "DYNAMODB_TABLE": "penguin-health-org-config",
                "COGNITO_USER_POOL_ID": user_pool.user_pool_id,
            },
        )

        org_config_table.grant_read_write_data(admin_api_fn)
        admin_api_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query", "dynamodb:Scan", "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"],
            resources=[f"{org_config_table.table_arn}/index/*"],
        ))

        # ----- API Gateway HTTP API -----
        jwt_authorizer = HttpJwtAuthorizer(
            "CognitoAuthorizer",
            jwt_issuer=f"https://cognito-idp.{self.region}.amazonaws.com/{user_pool.user_pool_id}",
            jwt_audience=[app_client.user_pool_client_id],
        )

        integration = HttpLambdaIntegration("AdminApiIntegration", admin_api_fn)

        http_api = apigwv2.HttpApi(self, "AdminHttpApi",
            api_name="penguin-health-admin-api",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[
                    apigwv2.CorsHttpMethod.GET,
                    apigwv2.CorsHttpMethod.PUT,
                    apigwv2.CorsHttpMethod.POST,
                    apigwv2.CorsHttpMethod.DELETE,
                    apigwv2.CorsHttpMethod.OPTIONS,
                ],
                allow_headers=["Authorization", "Content-Type"],
                max_age=Duration.hours(1),
            ),
        )

        # Define all API routes
        routes = [
            ("GET",  "/api/organizations"),
            ("GET",  "/api/organizations/{orgId}"),
            ("GET",  "/api/organizations/{orgId}/rules"),
            ("GET",  "/api/organizations/{orgId}/rules/{ruleId}"),
            ("PUT",  "/api/organizations/{orgId}/rules/{ruleId}"),
            ("POST", "/api/organizations/{orgId}/rules"),
            ("GET",  "/api/organizations/{orgId}/rules-config"),
            ("PUT",  "/api/organizations/{orgId}/rules-config"),
        ]

        for method, path in routes:
            http_api.add_routes(
                path=path,
                methods=[getattr(apigwv2.HttpMethod, method)],
                integration=integration,
                authorizer=jwt_authorizer,
            )

        # ----- S3 Bucket for frontend -----
        frontend_bucket = s3.Bucket(self, "FrontendBucket",
            bucket_name="penguin-health-admin-ui",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ----- CloudFront Distribution -----
        api_origin = origins.HttpOrigin(
            f"{http_api.http_api_id}.execute-api.{self.region}.amazonaws.com",
        )

        distribution = cloudfront.Distribution(self, "AdminDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(frontend_bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
            ),
            additional_behaviors={
                "/api/*": cloudfront.BehaviorOptions(
                    origin=api_origin,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                ),
            },
            default_root_object="index.html",
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(0),
                ),
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(0),
                ),
            ],
            minimum_protocol_version=cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
        )

        # ----- Outputs -----
        CfnOutput(self, "UserPoolId",
            value=user_pool.user_pool_id,
            description="Cognito User Pool ID",
        )
        CfnOutput(self, "UserPoolClientId",
            value=app_client.user_pool_client_id,
            description="Cognito App Client ID",
        )
        CfnOutput(self, "ApiUrl",
            value=http_api.url or "",
            description="API Gateway URL",
        )
        CfnOutput(self, "CloudFrontUrl",
            value=f"https://{distribution.distribution_domain_name}",
            description="CloudFront Distribution URL",
        )
        CfnOutput(self, "FrontendBucketName",
            value=frontend_bucket.bucket_name,
            description="S3 bucket for frontend assets",
        )
        CfnOutput(self, "DistributionId",
            value=distribution.distribution_id,
            description="CloudFront Distribution ID (for cache invalidation)",
        )
