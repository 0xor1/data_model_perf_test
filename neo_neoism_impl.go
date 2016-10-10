package data_model_perf_test

import (
	"github.com/jmcvetta/neoism"
)

func NewNeoNeoismDataModelPerfTest(connectionString string) DataModelPerfTest {
	db, _ := neoism.Connect(connectionString)
	return &neoNeoismDataModelPerfTest{
		db: db,
	}
}

type neoNeoismDataModelPerfTest struct {
	db *neoism.Database
}

func (t *neoNeoismDataModelPerfTest) Name() string {
	return "neo4j neoism"
}

func (t *neoNeoismDataModelPerfTest) Setup() error {
	return wipeNeoNeoismDb(t.db)
}

func (t *neoNeoismDataModelPerfTest) CreateData(k, h int) error {
	return createPerfectKaryTreeInNeo(k, h, func(cmd string) error {
		return t.db.Cypher(&neoism.CypherQuery{Statement: cmd})
	})
}

func (t *neoNeoismDataModelPerfTest) NodeAIsADescendantOfNodeB(nodeA int, nodeB int) (bool, error) {
	res := []struct {
		IsDescendant bool `json:"isDescendant"`
	}{}
	err := t.db.Cypher(&neoism.CypherQuery{Statement: generateNodeAisADescendantOfNodeBNeoQuery(nodeA, nodeB), Result: &res})
	return len(res) == 1, err
}

func (t *neoNeoismDataModelPerfTest) IncrementValuesBeneath(node int) error {
	return t.db.Cypher(&neoism.CypherQuery{Statement: generateIncrementValuesBeneathNeoQuery(node)})
}

func (t *neoNeoismDataModelPerfTest) SumValuesBeneath(node int) (int, error) {
	res := []struct {
		Sum int `json:"sum"`
	}{}
	err := t.db.Cypher(&neoism.CypherQuery{Statement: generateSumValuesBeneathNeoQuery(node), Result: &res})
	if len(res) == 1 {
		return res[0].Sum, err
	}
	return 0, err
}

func (t *neoNeoismDataModelPerfTest) GetAncestralChainOf(node int) ([]int, error) {
	ancestorsReceiver := []struct {
		Id int `json:"id"`
	}{}
	err := t.db.Cypher(&neoism.CypherQuery{Statement:generateGetAncestralChainOf(node), Result: &ancestorsReceiver})
	ancestors := make([]int, 0, len(ancestorsReceiver))
	for i := len(ancestorsReceiver) - 1; i >= 0; i-- {
		ancestors = append(ancestors, ancestorsReceiver[i].Id)
	}
	return ancestors, err
}

func (t *neoNeoismDataModelPerfTest) TearDown() error {
	return wipeNeoNeoismDb(t.db)
}
