"""
Admin UI construct: Cognito, API Gateway, Lambda, S3, CloudFront
for the organization admin dashboard.
"""

import os
from aws_cdk import (
    Duration,
    RemovalPolicy,
    BundlingOptions,
    aws_cognito as cognito,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_apigatewayv2 as apigwv2,
)
from aws_cdk.aws_apigatewayv2_integrations import HttpLambdaIntegration
from aws_cdk.aws_apigatewayv2_authorizers import HttpJwtAuthorizer
from constructs import Construct

import config
from components.bundler import CopyFileBundler, MultiFileBundler


class AdminUi(Construct):

    def __init__(self, scope: Construct, id: str, *,
                 org_config_table: dynamodb.ITable,
                 validation_results_table: dynamodb.ITable) -> None:
        super().__init__(scope, id)

        # ----- Cognito -----
        self.user_pool = cognito.UserPool(self, "AdminUserPool",
            user_pool_name=f"{config.PROJECT_NAME}-admin-pool",
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
            # Custom attributes for RBAC
            custom_attributes={
                "organization_id": cognito.StringAttribute(
                    mutable=False,  # Immutable - only admins can set via Admin API
                ),
            },
        )

        self.app_client = self.user_pool.add_client("AdminAppClient",
            user_pool_client_name=f"{config.PROJECT_NAME}-admin-app",
            auth_flows=cognito.AuthFlow(
                user_srp=True,
            ),
            id_token_validity=Duration.hours(1),
            access_token_validity=Duration.hours(1),
            refresh_token_validity=Duration.days(30),
            # Note: By NOT specifying read_attributes/write_attributes,
            # Cognito will include all readable attributes in tokens by default.
            # This is safer for compatibility. The custom:organization_id attribute
            # is already set as mutable=False on the User Pool, so users can't modify it.
        )

        cognito.CfnUserPoolGroup(self, "AdminsGroup",
            user_pool_id=self.user_pool.user_pool_id,
            group_name="Admins",
            description="Admin users with full access to organization configuration",
        )

        # ----- Admin API Lambda -----
        lambda_api_dir = os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "api")

        self.api_function = _lambda.Function(self, "AdminApiFunction",
            function_name=f"{config.PROJECT_NAME}-admin-api",
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler="admin_api.lambda_handler",
            code=_lambda.Code.from_asset(
                lambda_api_dir,
                exclude=["*", "!admin_api.py", "!permissions.py"],
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_14.bundling_image,
                    local=MultiFileBundler([
                        os.path.join(lambda_api_dir, "admin_api.py"),
                        os.path.join(lambda_api_dir, "permissions.py"),
                    ]),
                ),
            ),
            timeout=Duration.seconds(60),  # Longer timeout for LLM calls
            memory_size=config.LAMBDA_DEFAULT_MEMORY_MB,
            environment={
                "DYNAMODB_TABLE": org_config_table.table_name,
                "COGNITO_USER_POOL_ID": self.user_pool.user_pool_id,
            },
        )

        org_config_table.grant_read_write_data(self.api_function)
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query", "dynamodb:Scan", "dynamodb:GetItem",
                     "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"],
            resources=[f"{org_config_table.table_arn}/index/*"],
        ))

        validation_results_table.grant_read_write_data(self.api_function)
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:Query", "dynamodb:Scan", "dynamodb:GetItem",
                     "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"],
            resources=[f"{validation_results_table.table_arn}/index/*"],
        ))

        # Bedrock permissions for LLM enhancement endpoints
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=[
                "arn:aws:bedrock:*::foundation-model/anthropic.*",
                "arn:aws:bedrock:*:*:inference-profile/*",
            ],
        ))

        # Lambda invoke permission for triggering validation runs
        self.api_function.add_to_role_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[
                f"arn:aws:lambda:{config.AWS_REGION}:*:function:{config.PROJECT_NAME}-rules-engine-rag"
            ],
        ))

        # ----- API Gateway HTTP API -----
        jwt_authorizer = HttpJwtAuthorizer(
            "CognitoAuthorizer",
            jwt_issuer=f"https://cognito-idp.{config.AWS_REGION}.amazonaws.com/{self.user_pool.user_pool_id}",
            jwt_audience=[self.app_client.user_pool_client_id],
        )

        integration = HttpLambdaIntegration("AdminApiIntegration", self.api_function)

        self.http_api = apigwv2.HttpApi(self, "AdminHttpApi",
            api_name=f"{config.PROJECT_NAME}-admin-api",
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

        routes = [
            ("GET",  "/api/organizations"),
            ("GET",  "/api/organizations/{orgId}"),
            ("GET",  "/api/organizations/{orgId}/rules"),
            ("GET",  "/api/organizations/{orgId}/rules/{ruleId}"),
            ("PUT",  "/api/organizations/{orgId}/rules/{ruleId}"),
            ("POST", "/api/organizations/{orgId}/rules"),
            ("GET",  "/api/organizations/{orgId}/rules-config"),
            ("PUT",  "/api/organizations/{orgId}/rules-config"),
            ("POST", "/api/organizations/{orgId}/rules/enhance-fields"),
            ("POST", "/api/organizations/{orgId}/rules/enhance-note"),
            ("GET",  "/api/organizations/{orgId}/validation-runs"),
            ("POST", "/api/organizations/{orgId}/validation-runs"),
            ("GET",  "/api/organizations/{orgId}/validation-runs/{runId}"),
            ("GET",  "/api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}"),
            ("PUT",  "/api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/confirm-finding"),
            ("PUT",  "/api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/mark-resolved"),
            ("PUT",  "/api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/mark-incorrect"),
            ("GET",  "/api/me/permissions"),
            ("GET",  "/api/organizations/{orgId}/users"),
            ("GET",  "/api/organizations/{orgId}/users/{email}"),
            ("PUT",  "/api/organizations/{orgId}/users/{email}"),
            ("DELETE", "/api/organizations/{orgId}/users/{email}"),
        ]

        for method, path in routes:
            self.http_api.add_routes(
                path=path,
                methods=[getattr(apigwv2.HttpMethod, method)],
                integration=integration,
                authorizer=jwt_authorizer,
            )

        # ----- S3 Bucket for frontend -----
        self.frontend_bucket = s3.Bucket(self, "FrontendBucket",
            bucket_name=f"{config.PROJECT_NAME}-admin-ui",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ----- CloudFront Distribution -----
        api_origin = origins.HttpOrigin(
            f"{self.http_api.http_api_id}.execute-api.{config.AWS_REGION}.amazonaws.com",
        )

        self.distribution = cloudfront.Distribution(self, "AdminDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(self.frontend_bucket),
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
