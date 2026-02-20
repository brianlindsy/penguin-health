#!/usr/bin/env python3
import aws_cdk as cdk
import config
from stacks.penguin_health_stack import PenguinHealthStack

app = cdk.App()

PenguinHealthStack(app, "PenguinHealth",
    env=cdk.Environment(region=config.AWS_REGION),
)

app.synth()
