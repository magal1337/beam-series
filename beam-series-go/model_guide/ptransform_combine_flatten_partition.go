package main

import (
	"context"
	"log"
	"math"
	"math/rand"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

type Employee struct {
  Id int
  Name string 
  JoiningDate string 
  DeptID string 
  IsActive bool 
}

func max(a, v int) int {
  return int(math.Max(float64(a),float64(v)))
}
type LastJoiningDateFn struct {}

func (fn *LastJoiningDateFn) CreateAccumulator() string {
  return "0000-00-00"
}
func (fn *LastJoiningDateFn) AddInput(acc string, element Employee) string {
  if element.JoiningDate >= acc {
    return element.JoiningDate
  } else {
    return acc
  }
}
func (fn *LastJoiningDateFn) MergeAccumulators(a string, b string) string {
  
  if a >= b {
    return a
  } else {
    return b
  }
}
func (fn *LastJoiningDateFn) ExtractOutput(acc string) string {
  return acc
}
func max_joining_date(a, b string) string {
  if (a <= b) {
    return b
  } else {
    return a
  }
}

func partition_fn(emp Employee) int {
  return rand.Intn(3)
}

func init() {
  register.Function2x1(max)
  register.Function1x1(partition_fn)
  register.Combiner2[string,Employee](&LastJoiningDateFn{})
}
func main() {


  beam.Init()

  p := beam.NewPipeline()

  s := p.Root()

  employees := []Employee{
    Employee{
      0,
      "John C",
      "2000-01-01",
      "D101",
      true,
    },
    Employee{
      1,
      "Tom D",
      "2002-02-01",
      "D102",
      true,
    },
    Employee{
      2,
      "Max",
      "2003-04-01",
      "D104",
      false,
    },
    Employee{
      3,
      "Bruce",
      "2003-05-01",
      "D102",
      false,
    },
    Employee{
      4,
      "Barry",
      "2003-04-22",
      "D101",
      false,
    },
    Employee{
      5,
      "Clark",
      "2001-04-01",
      "D103",
      false,
    },
    Employee{
      6,
      "Bryan C.",
      "2010-07-01",
      "D104",
      true,
    },
    }
     
  // pre-built combine, simple combine and complex combine, partition and flatten example
  employees_pcoll := beam.CreateList(s,employees)
  // combine globally
  employees_id := beam.ParDo(s,func(element Employee) (int) {
    return element.Id
  },employees_pcoll)

  max_id_employee := beam.Combine(s,max,employees_id)
  // KV PCollection
  employees_kv := beam.ParDo(s, func(element Employee) (string,int) {
    return element.DeptID, element.Id
  },employees_pcoll)
  // Simple Pre-built combine
  max_id_by_dept := beam.CombinePerKey(s,max,employees_kv)
  // Complex combine
  employees_kv_full := beam.ParDo(s, func(element Employee) (string,Employee) {
    return element.DeptID, element
  },employees_pcoll)
  max_joining_date_by_dept := beam.CombinePerKey(s,&LastJoiningDateFn{},employees_kv_full)
  pcoll_groups := beam.Partition(s,3,partition_fn,employees_pcoll)
  pcoll_union := beam.Flatten(s,pcoll_groups...)
  debug.Print(s,max_id_employee)
  debug.Print(s,max_id_by_dept)
  debug.Print(s,max_joining_date_by_dept)
  debug.Print(s,pcoll_groups[1])
  debug.Print(s,pcoll_union)

  if err := beamx.Run(context.Background(), p); err != nil {
    log.Fatalf("Failed to execute job: %v",err)
  }
}
