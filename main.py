# from janis_runner.engines.cromwell.cromwellconfiguration import CromwellConfiguration
# from janis_runner.engines.cromwell.main import Cromwell
# from janis_runner.engines.engine import AsyncTask
#
# config = CromwellConfiguration(
#     aws=CromwellConfiguration.AWS(
#         region="ap-southeast-2",
#         auths=[
#             CromwellConfiguration.AWS.Auth(
#                 scheme="custom_keys", access_key="", secret_key=""
#             )
#         ],
#     ),
#     engine=CromwellConfiguration.Engine(s3=True)
#     # backend=CromwellConfiguration.Backend(
#     #     default="singularity",
#     #     providers={"singularity": CromwellConfiguration.Backend.Provider.slurm_singularity()}
#     # ),
# )
# path = "config.conf"
# with open(path, "w+") as f:
#     print("Configuration: \n" + "\n\t".join(config.output().splitlines()))
#     f.write(config.output())
#
# c = Cromwell(config_path=path, identifier="test")
# # c = CWLTool()
# c.start_engine()
#
#
# def handler(task, status, outputs):
#     print(outputs)
#     c.stop_engine()
#
#
# wdl = """
# task hello {
#   String name = "World!"
#
#   command {
#     echo 'Hello, ${name}!'
#   }
#   runtime {
#     docker: "ubuntu:latest"
#   }
#   output {
#     File response = stdout()
#   }
# }
#
# workflow test {
#   call hello
# }"""
#
# wdl_aws = """
# task cat_string {
#   String str
#
#   command {
#     cat ${str}
#   }
#   output {
#     File response = stdout()
#   }
# }
#
# workflow test {
#   File inp = "s3://shepherd-test-data/hello.txt"
#   File inpstr = read_string(inp)
#   call cat_string { input: str=inpstr }
# }"""
#
# cwl = """
# cwlVersion: v1.0
# class: CommandLineTool
# baseCommand: echo
# inputs:
#     name:
#         type: string
#         default: "World"
#         inputBinding:
#           prefix: "Hello, "
# outputs:
#     out: stdout
#
# requirements:
#     DockerRequirement:
#         dockerPull: "ubuntu:latest"
# """
#
#
# def onerror(task):
#     c.stop_engine()
#
#
# print("starting async task")
# AsyncTask(
#     tid="simple", engine=c, source=wdl_aws, handler=handler, onerror=onerror
# ).start()
# print("Task has been scheduled")
#
# # Task(
# #     engine=c,
# #     source_path="/Users/franklinmichael/Desktop/workflows-for-testing/wgs/docker/whole_genome_germline.cwl",
# #     dependencies_path="/Users/franklinmichael/Desktop/workflows-for-testing/wgs/docker/tools.zip",
# #     input_paths=["/Users/franklinmichael/Desktop/workflows-for-testing/wgs/whole_genome_germline-local.yml"],
# #     handler=handler
# # ).start()
