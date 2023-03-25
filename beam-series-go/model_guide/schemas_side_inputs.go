package main

import (
	"context"
	"log"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
  //"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

type Account struct {
    Name   string `beam:"name"`
    Age    int    `beam:"age"`
    Gender string `beam:"gender"`
    Email  string `beam:"email"`
}

type Scores struct {
    Account   string `beam:"account"`
    Date    string    `beam:"date"`
    Score int `beam:"score"`
}

type AccountScores struct {
    Account   string `beam:"account"`
    Date    string    `beam:"date"`
    Score int `beam:"score"`
    AccountName string `beam:"account_name"` 
}

func to_account(record string, emit func(Account)) {
  entities := strings.Split(record,",")
  age, _ := strconv.Atoi(entities[1])
  if (entities[0] != "Name"){
    emit(Account{
        Name:   entities[0],
        Age:    age,
        Gender: entities[2],
        Email:  entities[3],
    },
  )
  }
}

func to_score(record string, emit func(Scores)) {
  entities := strings.Split(record,",")
  score, _ := strconv.Atoi(entities[2])

  if (entities[0] != "Account") {

    emit(Scores{
        Account:   entities[0],
        Date:    entities[1],
        Score: score,
      },
    )
  }
}

func broad_cast_join(score_entity Scores, broad_table func(*Account) bool) AccountScores {
  var account Account
  var target_name string

  for broad_table(&account) {
    if (account.Email == score_entity.Account) {
      target_name = account.Name
    }
  }
  return AccountScores{
    Account: score_entity.Account,
    Date: score_entity.Date,
    Score: score_entity.Score,
    AccountName: target_name,
  } 
}

func init() {
}
func main() {


  beam.Init()

  p := beam.NewPipeline()

  s := p.Root()
  accounts_pcoll := textio.Read(s,"./data/accounts.csv")
  accounts := beam.ParDo(s, to_account, accounts_pcoll)
  
  scores_pcoll := textio.Read(s,"./data/scores.csv")
  scores := beam.ParDo(s, to_score, scores_pcoll)
  
  accounts_score := beam.ParDo(s,broad_cast_join,scores,beam.SideInput{Input: accounts})
  // combine globallys
  // KV PCollection
  /*
  employees_kv := beam.ParDo(s, func(element Employee) (string,int) {
    return element.DeptID, element.Id
  },employees_pcoll)
  // Simple Pre-built combine
  max_id_by_dept := beam.CombinePerKey(s,max,employees_kv)
  // Complex combine
  */
  debug.Print(s,accounts)
  debug.Print(s,scores)
  debug.Print(s,accounts_score)

  if err := beamx.Run(context.Background(), p); err != nil {
    log.Fatalf("Failed to execute job: %v",err)
  }
}
