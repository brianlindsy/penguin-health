"""
JWKS hosting construct: public S3 + CloudFront for serving JWK Sets that
FHIR vendors (Credible, etc.) fetch to verify the private_key_jwt client
assertions our Lambda sends at token requests.

Layout:
    s3://phealth-fhir-jwks/{org_id}/jwks.json
        |
        v CloudFront (S3 origin access control, signed bucket policy)
        |
    https://{jwks_domain}/{org_id}/jwks.json

Bucket-name note: this bucket is INTENTIONALLY outside the
`penguin-health-*` wildcard used in audit_engine.py and admin_ui.py.
That wildcard grants several Lambdas read/write across every per-org
PHI bucket, so a Lambda compromise or bug could otherwise overwrite a
JWK Set and let an attacker substitute their public key. Keeping the
JWKS bucket under a different name (`phealth-fhir-jwks`) excludes it
from those wildcards by construction. Only the provisioning script's
IAM principal (a human) ever writes here.

Keys are written by `scripts/multi-org/provision_fhir_keypair.py`. Nothing
in this construct generates or rotates keys — it only stands up the
hosting surface.

The domain + ACM cert must be provisioned BEFORE deploying this construct:
    - Pick a hostname (e.g. fhir-keys.<your-domain>)
    - Request an ACM cert in us-east-1 (CloudFront requires us-east-1)
    - Validate the cert via DNS (CNAME record)
    - Pass the cert ARN and hostname to the construct

If `jwks_domain` and `cert_arn` are None, the construct still creates the
S3 bucket + a default-domain CloudFront distribution (https://d123...
.cloudfront.net) — useful for testing but not registered with vendors,
because changing the cloudfront.net hostname later breaks the JWKS URL
that's on file with them.
"""

from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_certificatemanager as acm,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_s3 as s3,
)
from constructs import Construct

import config


class JwksHosting(Construct):

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        jwks_domain: str | None = None,
        cert_arn: str | None = None,
    ) -> None:
        super().__init__(scope, id)

        # ----- S3 bucket -----
        # PHI never lives in this bucket — only public JWKs. We still block
        # direct public access; CloudFront reads via Origin Access Control.
        # Name chosen to NOT match the `penguin-health-*` wildcard granted
        # to several Lambdas (see the module-level docstring above).
        self.bucket = s3.Bucket(
            self,
            "FhirJwksBucket",
            bucket_name="phealth-fhir-jwks",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ----- CloudFront distribution -----
        # Short TTL: when we rotate keys we want vendors to refetch quickly,
        # but not so short that every Credible token verification hits S3.
        cache_policy = cloudfront.CachePolicy(
            self,
            "FhirJwksCachePolicy",
            cache_policy_name=f"{config.PROJECT_NAME}-fhir-jwks-cache",
            default_ttl=Duration.minutes(5),
            min_ttl=Duration.seconds(0),
            max_ttl=Duration.minutes(15),
            enable_accept_encoding_gzip=True,
        )

        distribution_kwargs = {
            "default_behavior": cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(self.bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cache_policy,
                allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD,
            ),
            "minimum_protocol_version": cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
            "comment": "Penguin Health JWKS hosting for FHIR private_key_jwt",
        }

        if jwks_domain and cert_arn:
            certificate = acm.Certificate.from_certificate_arn(
                self, "FhirJwksCertificate", cert_arn
            )
            distribution_kwargs["domain_names"] = [jwks_domain]
            distribution_kwargs["certificate"] = certificate

        self.distribution = cloudfront.Distribution(
            self, "FhirJwksDistribution", **distribution_kwargs
        )

        # Save the canonical public URL for cross-stack reference. If a
        # custom domain wasn't provided, falls back to the cloudfront.net
        # one — fine for testing, not registerable with vendors.
        if jwks_domain:
            self.public_base_url = f"https://{jwks_domain}"
        else:
            self.public_base_url = (
                f"https://{self.distribution.distribution_domain_name}"
            )
