package data_model_perf_test

import (
	"database/sql"
	"fmt"
)

func NewSqlSpDataModelPerfTest(db *sql.DB) DataModelPerfTest {
	return &sqlSpDataModelPerfTest{
		db: db,
	}
}

type sqlSpDataModelPerfTest struct {
	db *sql.DB
}

func (t *sqlSpDataModelPerfTest) Name() string {
	return "sql SP"
}

func (t *sqlSpDataModelPerfTest) Setup() error {
	return wipeSqlDb(t.db)
}

func (t *sqlSpDataModelPerfTest) CreateData(k, h int) error {
	return createPerfectKaryTreeInSql(k, h, t.db)
}

func (t *sqlSpDataModelPerfTest) NodeAIsADescendantOfNodeB(nodeA int, nodeB int) (bool, error) {
	var res bool
	row := t.db.QueryRow(fmt.Sprintf("CALL nodeAIsADescendantOfNodeB(%d, %d)", nodeA, nodeB))
	err := row.Scan(&res)
	return res, err
}

func (t *sqlSpDataModelPerfTest) IncrementValuesBeneath(node int) error {
	_, err := t.db.Exec(fmt.Sprintf("CALL IncrementValuesBeneath(%d)", node))
	return err
}

func (t *sqlSpDataModelPerfTest) SumValuesBeneath(node int) (int, error) {
	var res int
	row := t.db.QueryRow(fmt.Sprintf("CALL SumValuesBeneath(%d)", node))
	err := row.Scan(&res)
	return res, err
}

func (t *sqlSpDataModelPerfTest) GetAncestralChainOf(node int) ([]int, error) {
	incrementSize := 10
	ancestors := make([]int, 0, incrementSize)
	appendAncestors := func(ancestor int) {
		if len(ancestors) == cap(ancestors) {
			ancestors = append(make([]int, 0, len(ancestors)+incrementSize), ancestors...)
		}
		ancestors = append(ancestors, ancestor)
	}
	rows, err := t.db.Query(fmt.Sprintf("CALL GetAncestralChainOf(%d)", node))
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var res int
		if err := rows.Scan(&res); err != nil {
			return nil, err
		}
		appendAncestors(res)
	}
	return ancestors, nil
}

func (t *sqlSpDataModelPerfTest) TearDown() error {
	return wipeSqlDb(t.db)
}
