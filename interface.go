package data_model_perf_test

type DataModelPerfTest interface {
	Name() string
	Setup() error
	CreateData(k, h int) error
	NodeAIsADescendantOfNodeB(nodeA int, nodeB int) (bool, error)
	IncrementValueOfNodeAndAllOfItsDescendants(node int) error
	SumValuesBeneath(node int) (int, error)
	GetAncestralChainOf(node int) ([]int, error)
	TearDown() error
}
