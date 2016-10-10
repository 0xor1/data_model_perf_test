package data_model_perf_test

import (
	"database/sql"
	"fmt"
)

func NewSqlDataModelPerfTest(db *sql.DB) DataModelPerfTest {
	return &sqlDataModelPerfTest{
		db: db,
	}
}

type sqlDataModelPerfTest struct {
	db *sql.DB
}

func (t *sqlDataModelPerfTest) Name() string {
	return "sql"
}

func (t *sqlDataModelPerfTest) Setup() error {
	return wipeSqlDb(t.db)
}

func (t *sqlDataModelPerfTest) CreateData(k, h int) error {
	return createPerfectKaryTreeInSql(k, h, t.db)
}

func (t *sqlDataModelPerfTest) NodeAIsADescendantOfNodeB(nodeA int, nodeB int) (bool, error) {
	getParentOf := nodeA
	var err error
	for getParentOf != nodeB && getParentOf != -1 && err == nil {
		row := t.db.QueryRow(fmt.Sprintf("SELECT parent FROM nodes WHERE id = %d", getParentOf))
		err = row.Scan(&getParentOf)
	}
	if getParentOf == nodeB {
		return true, err
	}
	return false, err
}

func (t *sqlDataModelPerfTest) IncrementValuesBeneath(node int) error {
	_, err := t.db.Exec(fmt.Sprintf("UPDATE nodes SET value = value + 1 WHERE parent = %d", node))
	if err != nil {
		return err
	}
	rows, err := t.db.Query(fmt.Sprintf("SELECT id FROM nodes WHERE parent = %d", node))
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return err
	}
	for rows.Next() {
		var nextNode int
		if err := rows.Scan(&nextNode); err != nil {
			return err
		}
		if err := t.IncrementValuesBeneath(nextNode); err != nil {
			return err
		}
	}
	return nil
}

func (t *sqlDataModelPerfTest) SumValuesBeneath(node int) (int, error) {
	var sum int
	rows, err := t.db.Query(fmt.Sprintf("SELECT id, value FROM nodes WHERE parent = %d", node))
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return sum, err
	}
	for rows.Next() {
		var nextNode int
		var val int
		if err := rows.Scan(&nextNode, &val); err != nil {
			return sum, err
		}
		sum += val
		if val, err := t.SumValuesBeneath(nextNode); err != nil {
			return sum, err
		} else {
			sum += val
		}
	}
	return sum, nil
}

func (t *sqlDataModelPerfTest) GetAncestralChainOf(node int) ([]int, error) {
	getParentOf := node
	incrementSize := 10
	ancestors := make([]int, 0, incrementSize)
	appendAncestors := func(ancestor int) {
		if len(ancestors) == cap(ancestors) {
			ancestors = append(make([]int, 0, len(ancestors)+incrementSize), ancestors...)
		}
		ancestors = append(ancestors, ancestor)
	}
	for getParentOf != -1 {
		row := t.db.QueryRow(fmt.Sprintf("SELECT parent FROM nodes WHERE id = %d", getParentOf))
		if err := row.Scan(&getParentOf); err != nil {
			return nil, err
		}
		appendAncestors(getParentOf)
	}
	if ancestors[len(ancestors)-1] == -1 {
		ancestors = ancestors[:len(ancestors)-1]
	}
	for i := len(ancestors)/2 - 1; i >= 0; i-- {
		opp := len(ancestors) - 1 - i
		ancestors[i], ancestors[opp] = ancestors[opp], ancestors[i]
	}
	return ancestors, nil
}

func (t *sqlDataModelPerfTest) TearDown() error {
	return wipeSqlDb(t.db)
}
