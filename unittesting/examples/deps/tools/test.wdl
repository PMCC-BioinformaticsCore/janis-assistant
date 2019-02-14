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