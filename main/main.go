package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	dmpt "github.com/robsix/data_model_perf_test"
	"github.com/robsix/golog"
)

func main() {
	log := golog.NewLog(golog.Info, "15:04:05.00", 20)
	db, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/dmpt?multiStatements=true")
	if err != nil {
		log.Fatal("Failed to open sql database err: %v", err)
	}
	dmpt.RunTests(
		[]dmpt.DataModelPerfTest{
			dmpt.NewNeoBoltDataModelPerfTest("bolt://neo4j:root@localhost:7687", 20),
			dmpt.NewNeoNeoismDataModelPerfTest("http://neo4j:root@localhost:7474"),
			dmpt.NewSqlDataModelPerfTest(db),
		},
		[]int{3/*, 4, 5, 6, 7, 8, 9, 10*/},
		[]int{3, 4, 5, 6, 7/*, 8, 9, 10*/},
		log,
	)
	log.Warning("TESTING COMPLETE: Press enter to exit")
	fmt.Scanln()
}
