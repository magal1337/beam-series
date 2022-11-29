package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

type Account struct {
  Name string
  Age int 
  Gender string 
  Email string 
  IsActive bool 
}

type AccountWithStatus struct {
  Account
  Status string
}

type GenerateAccountStatus struct{}

func (fn *GenerateAccountStatus) StartBundle(emit func(AccountWithStatus)) {
  fmt.Println("Iniciando Bundle...")
}

func (fn *GenerateAccountStatus) ProcessElement(account Account, emit func(AccountWithStatus)) {

  if account.IsActive == true {
    emit(AccountWithStatus{account,"approved"})
  } else {
    emit(AccountWithStatus{account,"rejected"})
  }

}

func (fn *GenerateAccountStatus) FinishBundle(emit func(AccountWithStatus)) {
  fmt.Println("Finalizando Bundle...")
  
}

func init() {
  register.DoFn2x0[Account,func(AccountWithStatus)] (&GenerateAccountStatus{})
  register.Emitter1[AccountWithStatus]()
}

func main() {


  beam.Init()

  p := beam.NewPipeline()

  s := p.Root()

  accounts := []Account{
    Account{
      "lucas",
      32,
      "M",
      "lucas@foo.com",
      true,
    },
    Account{
      "Fernando",
      38,
      "M",
      "fernando@foo.com",
      true,
    },
    Account{
      "maria",
      14,
      "F",
      "maria@foo.com",
      false,
    },
    }


  pcoll := beam.CreateList(s,accounts)
  /*
  filterElements := beam.ParDo(s, func(element Account, emit func(Account)) {

    if (element.Age >= 18) {
      emit(element)

    }
  },pcoll)
  */
  /*
  extractElements := beam.ParDo(s, func(element Account) string {
    return element.Email 
  },pcoll)
  */

  customParDo := beam.ParDo(s,&GenerateAccountStatus{},pcoll)
   
  debug.Print(s,customParDo)

  if err := beamx.Run(context.Background(), p); err != nil {
    log.Fatalf("Failed to execute job: %v",err)
  }
}
