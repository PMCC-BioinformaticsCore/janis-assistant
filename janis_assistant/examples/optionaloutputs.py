from janis_core import (
    CommandToolBuilder,
    ToolInput,
    ToolOutput,
    File,
    InputSelector,
    String,
    ToolArgument,
)

ToolWithOptionalOutput = CommandToolBuilder(
    tool="optional_output_tool",
    version="v0.1.0",
    container="ubuntu:latest",
    base_command=[],
    arguments=[ToolArgument("echo 1 > ", shell_quote=False)],
    inputs=[
        ToolInput(
            "outputFilename", String(optional=True), default="out.csv", position=1
        )
    ],
    outputs=[
        ToolOutput("out", File(optional=True), selector=InputSelector("outputFilename"))
    ],
)
