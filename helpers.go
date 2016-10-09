package data_model_perf_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/jmcvetta/neoism"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

func createPerfectKaryTreeInNeo(k, h int, execNeo func(string) error) error {
	lastNode := ((iPow(k, (h+1)) - 1) / (k - 1)) - 1
	err := execNeo(fmt.Sprintf("FOREACH(i IN RANGE(0, %d, 1) | CREATE (:NODE {id:i, value:i}))", lastNode))
	if err != nil {
		return err
	}
	err = execNeo(fmt.Sprintf("MATCH (a:NODE), (b:NODE) WHERE b.id = a.id * %d + 1 CREATE (a)-[:FIRST_CHILD]->(b)", k))
	if err != nil {
		return err
	}
	err = execNeo(fmt.Sprintf("MATCH (a:NODE), (b:NODE) WHERE b.id = a.id + 1 AND a.id %% %d <> 0 CREATE (a)-[:NEXT_SIBLING]->(b)", k))
	if err != nil {
		return err
	}
	return nil
}

func generateNodeAisADescendantOfNodeBNeoQuery(nodeA, nodeB int) string {
	return fmt.Sprintf("MATCH (b:NODE {id:%d})-[:FIRST_CHILD]->(:NODE)-[:FIRST_CHILD|:NEXT_SIBLING *]->(a:NODE {id:%d}) RETURN b.id UNION MATCH (b:NODE {id:%d})-[:FIRST_CHILD]->(a:NODE {id:%d}) RETURN b.id", nodeB, nodeA, nodeB, nodeA)
}

func generateIncrementValueOfNodeNeoQuery(node int) string {
	return fmt.Sprintf("MATCH (n:NODE {id:%d}) SET n.value = n.value + 1", node)
}

func generateIncrementValueOfAllDescendantsOfNeoQuery(node int) string {
	return fmt.Sprintf("MATCH (n:NODE {id:%d})-[:FIRST_CHILD|:NEXT_SIBLING *]->(a:NODE) SET a.value = a.value + 1", node)
}

func generateSumValuesBeneathNeoQuery(node int) string {
	return fmt.Sprintf("MATCH (NODE {id:%d})-[:FIRST_CHILD|:NEXT_SIBLING *]->(a:NODE) RETURN SUM(a.value) AS sum", node)
}

func generateGetAncestralChainOf(node int) string {
	return fmt.Sprintf("MATCH (parent:NODE)-[:FIRST_CHILD]->(child:NODE {id:%d}) RETURN parent.id AS id UNION MATCH (parent:NODE)-[:FIRST_CHILD]->(:NODE)-[:FIRST_CHILD|:NEXT_SIBLING *]->(child:NODE {id:%d}) RETURN parent.id AS id", node, node)
}

func wipeNeoBoltDb(driverPool bolt.DriverPool) error {
	conn, err := driverPool.OpenPool()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return err
	}
	_, err = conn.ExecNeo("MATCH (n) DETACH DELETE n", nil)
	_, err = conn.ExecNeo("DROP CONSTRAINT ON (n:NODE) ASSERT n.id IS UNIQUE", nil)
	_, err = conn.ExecNeo("CREATE CONSTRAINT ON (n:NODE) ASSERT n.id IS UNIQUE", nil)
	return err
}

func wipeNeoNeoismDb(db *neoism.Database) error {
	err := db.Cypher(&neoism.CypherQuery{Statement: "MATCH (n) DETACH DELETE n"})
	err = db.Cypher(&neoism.CypherQuery{Statement: "DROP CONSTRAINT ON (n:NODE) ASSERT n.id IS UNIQUE"})
	err = db.Cypher(&neoism.CypherQuery{Statement: "CREATE CONSTRAINT ON (n:NODE) ASSERT n.id IS UNIQUE"})
	return err
}

func createPerfectKaryTreeInSql(k, h int, db *sql.DB) error {
	totalNodes := (iPow(k, (h+1)) - 1) / (k - 1)
	batchSize := 1000
	for i := 0; i < totalNodes; {
		var cmd bytes.Buffer
		cmd.WriteString("INSERT INTO nodes (id, parent, value) VALUES ")
		for batchIndex := 0; batchIndex < batchSize; batchIndex++ {
			delimiter := ","
			if batchIndex+1 == batchSize || i == totalNodes-1 {
				delimiter = ";"
			}
			if i == 0 {
				cmd.WriteString(fmt.Sprintf("(%d,%d,%d)%v", i, -1, i, delimiter))
			} else {
				cmd.WriteString(fmt.Sprintf("(%d,%d,%d)%v", i, (i-1)/k, i, delimiter))
			}
			i++
			if i == totalNodes {
				break
			}
		}
		_, err := db.Exec(cmd.String())
		if err != nil {
			return err
		}
	}
	return nil
}

func wipeSqlDb(db *sql.DB) error {
	_, err := db.Exec(`
	DROP TABLE IF EXISTS nodes;
	CREATE TABLE nodes(
		id MEDIUMINT,
		parent MEDIUMINT,
		value MEDIUMINT,
		PRIMARY KEY (id),
		UNIQUE INDEX (parent, id)
	);`)
	return err
}

func iPow(a, b int) int {
	var result int = 1
	for 0 != b {
		if 0 != (b & 1) {
			result *= a
		}
		b >>= 1
		a *= a
	}
	return result
}
