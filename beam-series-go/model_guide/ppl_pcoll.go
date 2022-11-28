package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
  pcollSize = flag.Int("pcollSize",3,"pcollection size")
)

func GenerateSlice(size int) []int {
  tmpSlice := make([]int,size)
  for i :=0;i <size; i++ {
    tmpSlice[i] = i
  }
  return tmpSlice
}

func main() {

  flag.Parse()

  beam.Init()

  p := beam.NewPipeline()

  s := p.Root()

  // Integer types
  data_list := GenerateSlice(*pcollSize)

  // String types

  //data_list := []string{"banana","apple","orange"}
  
  // Map types 
  /*data_list := []map[string]any{
    map[string]any{
      "name": "lucas",
      "age": 22,
    },
    map[string]any{
      "name": "fernanda",
      "age": 28,
    },
  }*/

  // mix types
  //data_list := []any{1,2,3,"ioeowie",4}
  pcoll := beam.CreateList(s,data_list)

  beam.ParDo0(s, func(element any) {
    fmt.Println(element)
    return
  },pcoll)

  if err := beamx.Run(context.Background(), p); err != nil {
    log.Fatalf("Failed to execute job: %v",err)
  }
}
