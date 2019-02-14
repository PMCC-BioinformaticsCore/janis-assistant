import "tools/test.wdl" as T

workflow test {
  call T.hello
}