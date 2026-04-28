# Deploy admin UI frontend to S3 and invalidate CloudFront cache
#
# Usage:
#   .\scripts\admin-ui\deploy-frontend.ps1
#
# Prerequisites:
#   - CDK stack must be deployed (cdk deploy from infra/)
#   - npm dependencies installed in admin-ui/

$ErrorActionPreference = "Stop"

# Check and install prerequisites
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Host "npm not found. Attempting to install Node.js via winget..." -ForegroundColor Yellow
    winget install OpenJS.NodeJS.LTS --accept-source-agreements --accept-package-agreements
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Failed to install Node.js. Please install manually." -ForegroundColor Red
        exit 1
    }
    # Refresh PATH
    $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")
}
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    Write-Host "AWS CLI not found. Attempting to install via winget..." -ForegroundColor Yellow
    winget install Amazon.AWSCLI --accept-source-agreements --accept-package-agreements
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Failed to install AWS CLI. Please install manually." -ForegroundColor Red
        exit 1
    }
    # Refresh PATH
    $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
$FrontendDir = Join-Path $ProjectRoot "admin-ui"

$BucketName = "penguin-health-admin-ui"
$Region = "us-east-1"

Write-Host "Running tests..." -ForegroundColor Blue
Set-Location $FrontendDir
npm run test:run
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Building admin UI..." -ForegroundColor Blue
npm run build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Syncing to S3..." -ForegroundColor Blue
aws s3 sync dist/ "s3://$BucketName/" --delete --region $Region
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Invalidating CloudFront cache..." -ForegroundColor Blue

# Get distribution ID from CDK stack outputs
$DistId = aws cloudformation describe-stacks `
    --stack-name PenguinHealth `
    --query "Stacks[0].Outputs[?OutputKey=='DistributionId'].OutputValue" `
    --output text `
    --region $Region 2>$null

if ($DistId -and $DistId -ne "None") {
    aws cloudfront create-invalidation `
        --distribution-id $DistId `
        --paths "/*" `
        --region $Region | Out-Null
    Write-Host "CloudFront invalidation created" -ForegroundColor Green
} else {
    Write-Host "Could not find CloudFront distribution ID. Skipping invalidation." -ForegroundColor Red
}

Write-Host ""
Write-Host "Deploy complete!" -ForegroundColor Green

# Print CloudFront URL
$CfUrl = aws cloudformation describe-stacks `
    --stack-name PenguinHealth `
    --query "Stacks[0].Outputs[?OutputKey=='CloudFrontUrl'].OutputValue" `
    --output text `
    --region $Region 2>$null

if ($CfUrl -and $CfUrl -ne "None") {
    Write-Host "URL: $CfUrl" -ForegroundColor Blue
}
