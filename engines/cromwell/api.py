# Main cromwell
import requests


class CromwellApi:

    url_base = "http://localhost:8000/api/workflows/36"

    @staticmethod
    def create( source, inputs: list, dependencies, workflow_type="cwl"):
        """

        :param source:
        :param inputs:
        :param dependencies:
        :param workflow_type:
        :return:
        """

        # curl \
        #   -X POST "http://localhost:8000/api/workflows/v1" \
        #   -H "accept: application/json" \
        #   -H "Content-Type: multipart/form-data" \
        #   -F "workflowSource=@whole_genome_germline.cwl;type=" \
        #   -F "workflowInputs=@whole_genome_germline-local.yml;type=" \
        #   -F "workflowDependencies=@tools-gatk4.0.1.2.zip;type=application/zip

        url = CromwellApi.url_base

        max_dependencies = 5
        if len(inputs) > max_dependencies:
            raise Exception("Too many input dependencies (input yaml files). Proposed: automatic merge into one file.")


        files = {
            "workflowSource": open(source, "rb"),
            "workflowDependencies": open(dependencies, "rb"),
        }

        for i in range(len(dependencies)):
            k = "workflowInputs" + ("" if i == 0 else f"_{i+1}")
            files[k] = open(inputs[i], "rb")

        r = requests.post(url, files=files)
        print(r.json())



    @staticmethod
    def poll(identifier):
        pass
