"""
    REGEX used from Cromwell Execution Engine:

        - https://github.com/broadinstitute/cromwell/blob/1898d8103a06d160dc721d464862313e78ee7a2c/dockerHashing/src/main/scala/cromwell/docker/DockerHashResult.scala#L7
        - https://github.com/broadinstitute/cromwell/blob/323f0cd829c80dad7bdbbc0b0f6de2591f3fea72/dockerHashing/src/main/scala/cromwell/docker/DockerImageIdentifier.scala#L36-L60

        Copyright (c) 2015, Broad Institute, Inc.
        All rights reserved.

        Redistribution and use in source and binary forms, with or without
        modification, are permitted provided that the following conditions are met:

        * Redistributions of source code must retain the above copyright notice, this
          list of conditions and the following disclaimer.

        * Redistributions in binary form must reproduce the above copyright notice,
          this list of conditions and the following disclaimer in the documentation
          and/or other materials provided with the distribution.

        * Neither the name Broad Institute, Inc. nor the names of its
          contributors may be used to endorse or promote products derived from
          this software without specific prior written permission.

        THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
        AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
        IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
        DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
        FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
        DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
        SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
        CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
        OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
        OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE
"""

from re import compile

docker_hash_regex = compile("([a-zA-Z0-9_+.-]+):([a-zA-Z0-9]+)")

docker_string_regex = compile(
    """
   (?x)                                     # Turn on comments and whitespace insensitivity

   (                                        # Begin capturing group for name
      [a-z0-9]+(?:[._-][a-z0-9]+)*          # API v2 name component regex - see https://docs.docker.com/registry/spec/api/#/overview
      (?::[0-9]+)?                          # Optional port
      (?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*    # Optional additional name components separated by /
   )                                        # End capturing group for name

   (?:   
      :                                     # Tag separator. ':' is followed by a tag

      (                                     # Begin capturing group for reference 
        [A-Za-z0-9]+(?:[-.:_A-Za-z0-9]+)*   # Reference
      )                                     # End capturing group for reference  
   )?
   (?:   
      @                                     # Tag separator '@' is followed by a digest

      (                                     # Begin capturing group for reference 
        [A-Za-z0-9]+(?:[-.:_A-Za-z0-9]+)*   # Reference
      )                                     # End capturing group for reference  
   )?
   """.strip()
)
