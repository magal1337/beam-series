package main

import (
	"context"
	"fmt"
	"log"
  "flag"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

var (
  pcollSize = flag.Int("pcollSize",3,"pcollection size")
)
func GenerateSlice(size int) ([]int){
  tmpSlice := make([]int,size)
  for i :=0; i < size; i++ {
    tmpSlice[i] = i
  }
  return tmpSlice
}
func main(){
  flag.Parse()
  beam.Init()
  p := beam.NewPipeline()
  s:= p.Root()

  // Integer values
  data_list := GenerateSlice(*pcollSize)

  // String Values

  //data_list := []string{"banana","apple","orange"}
  
  // Map Types
  /*
  data_list :=[]map[string]any{ 
    map[string]any{
      "name": "lucas",
      "age": 14,
      "gender": "M",
    },
    map[string]any{
      "name": "fernando",
      "age": 18,
      "gender": "M",
    },
  }
  */
  
  // element type restriction
  //data_list := []any{1,2,3,"pipopo",4}
  



  pcoll := beam.CreateList(s,data_list)


  beam.ParDo0(s, func(element any) {
    fmt.Println(element)
    return
  },pcoll)
  

  if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
