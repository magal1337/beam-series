package main

import (
	"context"
	"log"

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

type Dept struct {
  Id string
  DeptName string
}

func FlatJoin(key string, employeeIter func(*Employee) bool,deptIter func(*Dept) bool, emit func(string,Employee)) {
  var dept Dept
  var dept_name string
  for deptIter(&dept) {
    dept_name = dept.DeptName
  }

  var employee Employee
  for employeeIter(&employee) {
    emit(dept_name,employee)
  }
}

func init() {
  register.Function4x0(FlatJoin)
  register.Emitter2[string,Employee]()
  register.Iter1[Employee]()
  register.Iter1[Dept]()
}

func main() {

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
     
  dept := []Dept{
      Dept{
        "D101",
        "Support",
      },
      Dept{
        "D102",
        "HR",
      },
      Dept{
        "D103",
        "Marketing",
      },
      Dept{
        "D104",
        "Sells",
      },
  }

  beam.Init()

  p := beam.NewPipeline()

  s := p.Root()



  employees_pcoll := beam.CreateList(s,employees)
  dept_pcoll := beam.CreateList(s,dept)

  employees_kv := beam.ParDo(s, func(element Employee) (string,Employee) {
    return element.DeptID,element
  },employees_pcoll)
  
  dept_kv := beam.ParDo(s, func(element Dept) (string,Dept) {
    return element.Id,element
  },dept_pcoll)

  joined_cogroup := beam.CoGroupByKey(s,employees_kv,dept_kv)

  flatten_joined := beam.ParDo(s,FlatJoin,joined_cogroup)

  group_by_dept := beam.GroupByKey(s,flatten_joined)

  min_joining_date_per_dept := beam.ParDo(s, func(key string, element func(*Employee) bool) (string,string) {
      min_date := "9999-99-99"
      var employee Employee
      for element(&employee) {
        if (employee.JoiningDate <= min_date) {
          min_date = employee.JoiningDate
        }
      }
      return key, min_date
  },group_by_dept)

   
  debug.Print(s,min_joining_date_per_dept)

  if err := beamx.Run(context.Background(), p); err != nil {
    log.Fatalf("Failed to execute job: %v",err)
  }
}
