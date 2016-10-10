package data_model_perf_test

import (
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"io"
	"time"
)

func NewNeoBoltDataModelPerfTest(connectionString string, maxConnections int) DataModelPerfTest {
	driverPool, _ := bolt.NewDriverPool(connectionString, maxConnections)
	return &neoBoltDataModelPerfTest{
		driverPool: driverPool,
	}
}

type neoBoltDataModelPerfTest struct {
	driverPool bolt.DriverPool
}

func (t *neoBoltDataModelPerfTest) Name() string {
	return "neo4j bolt"
}

func (t *neoBoltDataModelPerfTest) Setup() error {
	return wipeNeoBoltDb(t.driverPool)
}

func (t *neoBoltDataModelPerfTest) CreateData(k, h int) error {
	conn, err := t.driverPool.OpenPool()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return err
	}
	err = createPerfectKaryTreeInNeo(k, h, func(cmd string) error {
		_, err := conn.ExecNeo(cmd, nil)
		return err
	})
	return err
}

func (t *neoBoltDataModelPerfTest) NodeAIsADescendantOfNodeB(nodeA int, nodeB int) (bool, error) {
	conn, err := t.driverPool.OpenPool()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return false, err
	}
	rows, err := conn.QueryNeo(generateNodeAisADescendantOfNodeBNeoQuery(nodeA, nodeB), nil)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return false, err
	}
	if rows != nil {
		_, _, err = rows.NextNeo()
		if err == nil {
			return true, nil
		} else if err == io.EOF {
			return false, nil
		}
	}
	return false, nil
}

func (t *neoBoltDataModelPerfTest) IncrementValueOfNodeAndAllOfItsDescendants(node int) error {
	conn, err := t.driverPool.OpenPool()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return err
	}
	_, err = conn.ExecNeo(generateIncrementValueOfNodeNeoQuery(node), nil)
	if err != nil {
		return err
	}
	_, err = conn.ExecNeo(generateIncrementValueOfAllDescendantsOfNeoQuery(node), nil)
	return err
}

func (t *neoBoltDataModelPerfTest) SumValuesBeneath(node int) (int, error) {
	conn, err := t.driverPool.OpenPool()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return 0, err
	}
	rows, err := conn.QueryNeo(generateSumValuesBeneathNeoQuery(node), nil)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return 0, err
	}
	sum := 0
	if rows != nil {
		data, _, err := rows.NextNeo()
		if err != nil {
			return 0, nil
		}
		sum = int(data[0].(int64))
	}
	return sum, nil
}

func (t *neoBoltDataModelPerfTest) GetAncestralChainOf(node int) ([]int, error) {
	conn, err := t.driverPool.OpenPool()
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryNeo(generateGetAncestralChainOf(node), nil)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return nil, err
	}
	incrementSize := 10
	ancestors := make([]int, 0, incrementSize)
	appendAncestors := func(ancestor int) {
		if len(ancestors) == cap(ancestors) {
			ancestors = append(make([]int, 0, len(ancestors)+incrementSize), ancestors...)
		}
		ancestors = append(ancestors, ancestor)
	}
	if rows != nil {
		data, _, err := rows.NextNeo()
		for err == nil {
			appendAncestors(int(data[0].(int64)))
			data, _, err = rows.NextNeo()
		}
		if err != io.EOF {
			return nil, err
		}
	}
	for i := len(ancestors)/2 - 1; i >= 0; i-- {
		opp := len(ancestors) - 1 - i
		ancestors[i], ancestors[opp] = ancestors[opp], ancestors[i]
	}
	return ancestors, nil
}

func (t *neoBoltDataModelPerfTest) TearDown() error {
	return wipeNeoBoltDb(t.driverPool)
}
