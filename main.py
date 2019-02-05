from engines.cromwell.main import Cromwell
from engines.engine import Task

c = Cromwell()
c.start_engine()


def handler(task, status, outputs):
    print(outputs)
    c.stop_engine()


# Task(
#     engine=c,
#     source="/Users/franklinmichael/source/shepherd/helloworld.wdl",
#     inputs="/Users/franklinmichael/source/shepherd/helloworld.json",
#     handler=handler
# ).start()

Task(
    engine=c,
    source="/Users/franklinmichael/Desktop/workflows-for-testing/wgs/docker/whole_genome_germline.cwl",
    dependencies="/Users/franklinmichael/Desktop/workflows-for-testing/wgs/docker/tools.zip",
    inputs="/Users/franklinmichael/Desktop/workflows-for-testing/wgs/whole_genome_germline-local.yml",
    handler=handler
).start()
