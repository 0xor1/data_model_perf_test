package data_model_perf_test

import (
	"github.com/robsix/golog"
	"time"
)

func RunTests(tests []DataModelPerfTest, ks []int, hs []int, log golog.Log) {
	for _, k := range ks {
		if !(k > 2) {
			log.Fatal("k must be greater than 2, k: %d", k)
		}
		for _, h := range hs {
			for _, test := range tests {

				totalNodes := (iPow(k, (h+1)) - 1) / (k - 1)
				lastNode := totalNodes - 1
				log.Warning("Running tests for: %v with k: %d h: %d totalNodes: %d", test.Name(), k, h, totalNodes)

				err := test.Setup()
				if err != nil {
					log.Fatal("Failed to setup for: %v err: %v", test.Name(), err)
				}

				err = test.CreateData(k, h)
				if err != nil {
					log.Fatal("Failed to create data for: %v k: %d h %d err: %v", test.Name(), k, h, err)
				}

				start := time.Now()
				AIsAncestorOfB, err := test.NodeAIsADescendantOfNodeB(lastNode, 0) //should return true every node is a descendant of the origin
				dur := time.Now().Sub(start)
				if err != nil {
					log.Fatal("Failed to run NodeAIsADescendantOfNodeB query for: %v err: %v", test.Name(), err)
				}
				if !AIsAncestorOfB {
					log.Fatal("Kary tree is not perfect, test.NodeAIsADescendantOfNodeB(lastNode, 0) should be true for: %v err: %v", test.Name(), err)
				}
				log.Info("%v test.NodeAIsADescendantOfNodeB(lastNode, 0) dur: %v", test.Name(), dur)

				start = time.Now()
				AIsAncestorOfB, err = test.NodeAIsADescendantOfNodeB(lastNode, 1) //should return false no one is a descendant of their uncle
				dur = time.Now().Sub(start)
				if err != nil {
					log.Fatal("Failed to run NodeAIsADescendantOfNodeB query for: %v err: %v", test.Name(), err)
				}
				if AIsAncestorOfB {
					log.Fatal("Kary tree is not perfect, test.NodeAIsADescendantOfNodeB(lastNode, 1) should be false for: %v err: %v", test.Name(), err)
				}
				log.Info("%v test.NodeAIsADescendantOfNodeB(lastNode, 1) dur: %v", test.Name(), dur)

				start = time.Now()
				err = test.IncrementValueOfNodeAndAllOfItsDescendants(0)
				dur = time.Now().Sub(start)
				if err != nil {
					log.Fatal("Failed to run IncrementValueOfNodeAndAllOfItsDescendants query for: %v err: %v", test.Name(), err)
				}
				log.Info("%v test.IncrementValueOfNodeAndAllOfItsDescendants(0) dur: %v", test.Name(), dur)

				start = time.Now()
				sum, err := test.SumValuesBeneath(0)
				dur = time.Now().Sub(start)
				if err != nil {
					log.Fatal("Failed to run SumValuesBeneath query for: %v err: %v", test.Name(), err)
				}
				expectedSum := ((totalNodes + 1) * (totalNodes / 2)) - 1
				if totalNodes%2 == 1 {
					expectedSum += (totalNodes / 2) + 1
				}
				if sum != expectedSum {
					log.Fatal("%v test.SumValuesBeneath(0) returned: %d but was expecting: %d", test.Name(), sum, expectedSum)
				}
				log.Info("%v test.SumValuesBeneath(0) dur: %v", test.Name(), dur)

				start = time.Now()
				ancestors, err := test.GetAncestralChainOf(lastNode)
				dur = time.Now().Sub(start)
				if err != nil {
					log.Fatal("Failed to run GetAncestralChainOf query for: %v err: %v", test.Name(), err)
				}
				if len(ancestors) != h {
					log.Fatal("%v test.GetAncestralChainOf(lastNode) incorrect number of parents in chain: %d but was expecting: %d", test.Name(), len(ancestors), h)
				}
				for i := len(ancestors); i >= 1; i-- {
					parent := ancestors[i-1]
					expectedParent := 0
					if i == len(ancestors) {
						expectedParent = (lastNode - 1) / k
					} else {
						expectedParent = (ancestors[i] - 1) / k
					}
					if expectedParent != parent {
						log.Fatal("%v test.GetAncestralChainOf(lastNode) incorrect parent in chain: %d but was expecting: %d", test.Name(), parent, expectedParent)
					}
				}
				log.Info("%v test.GetAncestralChainOf(lastNode) dur: %v", test.Name(), dur)

				err = test.TearDown()
				if err != nil {
					log.Fatal("Failed to tear down for: %v err: %v", test.Name(), err)
				}
			}
		}
	}
}
