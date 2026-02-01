#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.admin_ui_stack import AdminUiStack

app = cdk.App()

AdminUiStack(app, "PenguinHealthAdminUi",
    env=cdk.Environment(region="us-east-1")
)

app.synth()
