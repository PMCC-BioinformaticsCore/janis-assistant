from shepherd.engines.cromwell.configurations import CromwellConfiguration
from shepherd.engines.cromwell.main import Cromwell
from shepherd.engines.cwltool.main import CWLTool
from shepherd.engines.engine import AsyncTask

# config = CromwellConfiguration(
#     backend=CromwellConfiguration.Backend(
#         default="singularity",
#         providers={"singularity": CromwellConfiguration.Backend.Provider.slurm_singularity()}
#     ),
# )
# path = "config.conf"
# with open(path, "w+") as f:
#     f.write(config.output())

c = Cromwell(config_path=None)# path)
# c = CWLTool()
c.start_engine()


def handler(task, status, outputs):
    print(outputs)
    c.stop_engine()


wdl = """
task hello {
  String name = "World!"

  command {
    echo 'Hello, ${name}!'
  }
  runtime {
    docker: "ubuntu:latest"
  }
  output {
    File response = stdout()
  }
}

workflow test {
  call hello
}"""

cwl = """
cwlVersion: v1.0
class: CommandLineTool
baseCommand: echo
inputs:
    name:
        type: string
        default: "World"
        inputBinding:
          prefix: "Hello, "
outputs:
    out: stdout

requirements:
    DockerRequirement:
        dockerPull: "ubuntu:latest"
"""


def onerror(task):
    c.stop_engine()

print("starting async task")
AsyncTask(
    engine=c,
    source=cwl,
    handler=handler,
    onerror=onerror
).start()
print("Task has been scheduled")

# Task(
#     engine=c,
#     source_path="/Users/franklinmichael/Desktop/workflows-for-testing/wgs/docker/whole_genome_germline.cwl",
#     dependencies_path="/Users/franklinmichael/Desktop/workflows-for-testing/wgs/docker/tools.zip",
#     input_paths=["/Users/franklinmichael/Desktop/workflows-for-testing/wgs/whole_genome_germline-local.yml"],
#     handler=handler
# ).start()
