package data_model_perf_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/jmcvetta/neoism"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

func createPerfectKaryTreeInNeo(k, h int, execNeo func(string) error) error {
	return execNeo(fmt.Sprintf(`
	WITH %d AS k, %d AS h
	WITH k AS k, REDUCE(s = toFloat(0), x IN RANGE(1, h-1)|s + k^x) AS max_parent_id
	UNWIND RANGE(0, toInt(max_parent_id)) AS parent_id
	WITH k AS k, parent_id, k*parent_id+1 AS first_child_id
	MERGE (parent:NODE {id:parent_id, value:parent_id})
	MERGE (child:NODE {id: first_child_id, value:first_child_id})
	MERGE (parent) - [:FIRST_CHILD] -> (child)
	WITH k AS k, first_child_id, parent
	UNWIND RANGE(first_child_id + 1, first_child_id + k - 1) AS next_child_id
	MERGE (last_child:NODE {id:next_child_id -1, value:next_child_id -1})
	MERGE (next_child:NODE {id:next_child_id, value:next_child_id})
	MERGE (last_child) - [:NEXT_SIBLING] -> (next_child)
	MERGE (last_child) - [:PARENT] -> (parent)
	MERGE (next_child) - [:PARENT] -> (parent)
	`, k, h))
}

func generateNodeAisADescendantOfNodeBNeoQuery(nodeA, nodeB int) string {
	return fmt.Sprintf("MATCH (b:NODE)<-[:PARENT *1..]-(a:NODE {id:%d}) WITH b WHERE b.id = %d RETURN true AS isDescendant", nodeA, nodeB)
}

func generateIncrementValueOfNodeNeoQuery(node int) string {
	return fmt.Sprintf("MATCH (n:NODE {id:%d}) SET n.value = n.value + 1", node)
}

func generateIncrementValuesBeneathNeoQuery(node int) string {
	return fmt.Sprintf("MATCH (n:NODE {id:%d})<-[:PARENT *]-(a:NODE) SET a.value = a.value + 1", node)
}

func generateSumValuesBeneathNeoQuery(node int) string {
	return fmt.Sprintf("MATCH (:NODE {id:%d})<-[:PARENT *]-(a:NODE) RETURN SUM(a.value) AS sum", node)
}

func generateGetAncestralChainOf(node int) string {
	return fmt.Sprintf("MATCH (parent:NODE)<-[:PARENT *1..]-(child:NODE {id:%d}) RETURN parent.id AS id", node)
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
	);

	DROP FUNCTION IF EXISTS nodeAIsADescendantOfNodeB;
	CREATE FUNCTION nodeAIsADescendantOfNodeB(nodeA MEDIUMINT, nodeB MEDIUMINT) RETURNS BOOL NOT DETERMINISTIC
	BEGIN
		WHILE nodeA <> nodeB AND nodeA <> -1 DO
			SET nodeA = (SELECT parent FROM nodes WHERE id = nodeA);
		END WHILE;
		RETURN (SELECT nodeA = nodeB);
	END;

	DROP PROCEDURE IF EXISTS IncrementValuesBeneath;
	CREATE PROCEDURE IncrementValuesBeneath(node MEDIUMINT)
	BEGIN
		DECLARE tempIndexPtr MEDIUMINT DEFAULT 0;
		DECLARE tempIdsLen MEDIUMINT DEFAULT 0;

		DROP TEMPORARY TABLE IF EXISTS tempIncrementValuesBeneathIds;
	    	CREATE TEMPORARY TABLE tempIncrementValuesBeneathIds(
			id MEDIUMINT,
			PRIMARY KEY(id)
	    	);

		DROP TEMPORARY TABLE IF EXISTS tempIncrementValuesBeneathIds2;
	    	CREATE TEMPORARY TABLE tempIncrementValuesBeneathIds2(
			id MEDIUMINT,
			PRIMARY KEY(id)
	    	);

	    	INSERT INTO tempIncrementValuesBeneathIds (id) SELECT id FROM nodes WHERE parent = node;
	    	SET tempIdsLen = (SELECT COUNT(*) FROM tempIncrementValuesBeneathIds);

	    	WHILE tempIndexPtr < tempIdsLen DO
			INSERT INTO tempIncrementValuesBeneathIds2 (id) SELECT n.id FROM nodes AS n INNER JOIN (SELECT id FROM tempIncrementValuesBeneathIds LIMIT tempIndexPtr, 18446744073709551615) AS temp ON n.parent = temp.id;
			INSERT INTO tempIncrementValuesBeneathIds (id) SELECT id FROM tempIncrementValuesBeneathIds2;
			SET tempIndexPtr = tempIdsLen;
			SET tempIdsLen = (SELECT COUNT(*) FROM tempIncrementValuesBeneathIds);
			TRUNCATE TABLE tempIncrementValuesBeneathIds2;
		END WHILE;

		UPDATE nodes SET value = value + 1 WHERE id IN (SELECT id FROM tempIncrementValuesBeneathIds);
		DROP TEMPORARY TABLE IF EXISTS tempIncrementValuesBeneathIds;
		DROP TEMPORARY TABLE IF EXISTS tempIncrementValuesBeneathIds2;
	END;

	DROP PROCEDURE IF EXISTS SumValuesBeneath;
	CREATE PROCEDURE SumValuesBeneath(node MEDIUMINT)
	BEGIN
		DECLARE tempIndexPtr MEDIUMINT DEFAULT 0;
		DECLARE tempIdsLen MEDIUMINT DEFAULT 0;

		DROP TEMPORARY TABLE IF EXISTS tempSumValuesBeneathIds;
		CREATE TEMPORARY TABLE tempSumValuesBeneathIds(
			id MEDIUMINT,
			PRIMARY KEY(id)
		);

		DROP TEMPORARY TABLE IF EXISTS tempSumValuesBeneathIds2;
		CREATE TEMPORARY TABLE tempSumValuesBeneathIds2(
			id MEDIUMINT,
			PRIMARY KEY(id)
		);

		INSERT INTO tempSumValuesBeneathIds (id) SELECT id FROM nodes WHERE parent = node;
		SET tempIdsLen = (SELECT COUNT(*) FROM tempSumValuesBeneathIds);

		WHILE tempIndexPtr < tempIdsLen  DO
			INSERT INTO tempSumValuesBeneathIds2 (id) SELECT n.id FROM nodes AS n INNER JOIN (SELECT id FROM tempSumValuesBeneathIds LIMIT tempIndexPtr, 18446744073709551615) AS temp ON n.parent = temp.id;
			INSERT INTO tempSumValuesBeneathIds (id) SELECT id FROM tempSumValuesBeneathIds2;
			SET tempIndexPtr = tempIdsLen;
			SET tempIdsLen = (SELECT COUNT(*) FROM tempSumValuesBeneathIds);
			TRUNCATE TABLE tempSumValuesBeneathIds2;
		END WHILE;

		SELECT SUM(value) FROM nodes WHERE id IN (SELECT * FROM tempSumValuesBeneathIds);
		DROP TEMPORARY TABLE IF EXISTS tempSumValuesBeneathIds;
		DROP TEMPORARY TABLE IF EXISTS tempSumValuesBeneathIds2;
	END;

	DROP PROCEDURE IF EXISTS GetAncestralChainOf;
	CREATE PROCEDURE GetAncestralChainOf(node MEDIUMINT)
	BEGIN
		DROP TEMPORARY TABLE IF EXISTS tempGetAncestralChainOfIds;
		CREATE TEMPORARY TABLE tempGetAncestralChainOfIds(
			selectOrder MEDIUMINT NOT NULL AUTO_INCREMENT,
			id MEDIUMINT,
			PRIMARY KEY(selectOrder)
		);

		WHILE node <> -1  DO
			SET node = (SELECT parent FROM nodes WHERE id = node);
			IF node <> -1 THEN
				INSERT INTO tempGetAncestralChainOfIds (id) VALUES (node);
			END IF;
		END WHILE;

		SELECT id FROM tempGetAncestralChainOfIds ORDER BY selectOrder DESC;
		DROP TEMPORARY TABLE IF EXISTS tempGetAncestralChainOfIds;
	END;`)
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
