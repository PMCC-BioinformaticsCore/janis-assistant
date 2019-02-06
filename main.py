from engines.cromwell.configurations import CromwellConfiguration
from engines.cromwell.main import Cromwell
from engines.engine import Task
import json

# config = CromwellConfiguration(backend=CromwellConfiguration.Backend.slurm())
# path = "config.conf"
# with open(path, "w+") as f:
#     f.write(config.output())

c = Cromwell(config_path=None) # path)
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


Task(
    engine=c,
    source=wdl,
    handler=handler
).start()

# Task(
#     engine=c,
#     source_path="/Users/franklinmichael/Desktop/workflows-for-testing/wgs/docker/whole_genome_germline.cwl",
#     dependencies_path="/Users/franklinmichael/Desktop/workflows-for-testing/wgs/docker/tools.zip",
#     input_paths=["/Users/franklinmichael/Desktop/workflows-for-testing/wgs/whole_genome_germline-local.yml"],
#     handler=handler
# ).start()
