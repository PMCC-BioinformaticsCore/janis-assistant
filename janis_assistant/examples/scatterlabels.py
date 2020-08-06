from janis_core import (
    WorkflowBuilder,
    Array,
    String,
    ScatterDescription,
    StringFormatter,
)
from janis_unix import Echo

w = WorkflowBuilder("scatterlabel_test")

w.input("inps", Array(String), value=["test1,1", "test2,2", "test3,3"])

w.step(
    "print",
    Echo(inp=w.inps),
    scatter=ScatterDescription(["inp"], labels=["Printing 1", "Printing 2"]),
)

w.output("out", source=w.print, extension=".csv")

if __name__ == "__main__":
    w.translate("cwl")
