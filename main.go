package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	test1()
	fmt.Println()
	test2()
	fmt.Println()
	test3()
	fmt.Println()
	test4()
	fmt.Println()
	test5()
	fmt.Println()
	test6()
}

func test1() {
	fmt.Println("--- Test 1: Single Write and Read ---")
	nodeList := &NodeList{list: make(map[string]*Node)}
	node := NewNode("node1", []string{}, nodeList)

	groupA := NewPaxosGroup("groupA", node)

	groups := map[string]*PaxosGroup{
		"groupA": groupA,
	}
	tt := TrueTime{uncertainty: 5 * time.Millisecond}
	client := NewClient(groups, tt)

	fmt.Println("Writing users:1 = Alice to groupA...")
	err := client.Write("users:1", "Alice", "groupA")
	if err != nil {
		fmt.Printf("Write error: %v\n", err)
		return
	}

	fmt.Println("Reading users:1 from groupA...")
	val, ok := client.Read("users:1", "groupA")
	if !ok {
		fmt.Println("Value not found")
		return
	}

	fmt.Printf("Expected: Alice\n")
	fmt.Printf("Result: %s\n", val)
}

func test2() {
	fmt.Println("--- Test 2: Cross-group transaction ---")
	nodeList := &NodeList{list: make(map[string]*Node)}
	node1 := NewNode("node1", []string{}, nodeList)
	node2 := NewNode("node2", []string{}, nodeList)

	groupA := NewPaxosGroup("groupA", node1)
	groupB := NewPaxosGroup("groupB", node2)

	groups := map[string]*PaxosGroup{
		"groupA": groupA,
		"groupB": groupB,
	}

	tt := TrueTime{uncertainty: 5 * time.Millisecond}
	client := NewClient(groups, tt)

	fmt.Println("Writing users:2 = Bob to groupA & scores:2 = 100 to groupB...")
	err := client.RunTransaction(func(tx *Transaction) {
		tx.BufferWrite("users:2", "Bob", "groupA")
		tx.BufferWrite("scores:2", "100", "groupB")
	})

	if err != nil {
		fmt.Printf("Transaction error: %v\n", err)
		return
	}

	fmt.Println("Reading users:2 and scores:2...")
	valA, okA := client.Read("users:2", "groupA")
	valB, okB := client.Read("scores:2", "groupB")

	if !okA || !okB {
		fmt.Println("One or more values not found")
		return
	}

	fmt.Printf("Expected: users:2 = Bob, scores:2 = 100\n")
	fmt.Printf("Result: users:2 = %s, scores:2 = %s\n", valA, valB)
}

func test5() {
	fmt.Println("--- Test 5: Read-only transaction ---")
	nodeList := &NodeList{list: make(map[string]*Node)}
	node1 := NewNode("node1", []string{}, nodeList)
	node2 := NewNode("node2", []string{}, nodeList)

	groupA := NewPaxosGroup("groupA", node1)
	groupB := NewPaxosGroup("groupB", node2)

	groups := map[string]*PaxosGroup{
		"groupA": groupA,
		"groupB": groupB,
	}

	tt := TrueTime{uncertainty: 5 * time.Millisecond}
	client := NewClient(groups, tt)

	fmt.Println("Writing users:5 = Charlie to groupA & scores:5 = 200 to groupB...")
	err := client.RunTransaction(func(tx *Transaction) {
		tx.BufferWrite("users:5", "Charlie", "groupA")
		tx.BufferWrite("scores:5", "200", "groupB")
	})

	if err != nil {
		fmt.Printf("Transaction error: %v\n", err)
		return
	}

	fmt.Println("Performing ReadOnly multi-group transaction for users:5 and scores:5...")
	results, err := client.ReadOnly([]string{"users:5", "scores:5"}, []string{"groupA", "groupB"})
	if err != nil {
		fmt.Printf("ReadOnly error: %v\n", err)
		return
	}

	fmt.Printf("Expected: users:5 = Charlie, scores:5 = 200\n")
	fmt.Printf("Result: users:5 = %s, scores:5 = %s\n", results["users:5"], results["scores:5"])
}

func test3() {
	fmt.Println("--- Test 3: Abort on failure ---")
	nodeList := &NodeList{list: make(map[string]*Node)}
	node1 := NewNode("node1", []string{}, nodeList)
	node2 := NewNode("node2", []string{}, nodeList)

	groupA := NewPaxosGroup("groupA", node1)
	groupB := NewPaxosGroup("groupB", node2)

	groups := map[string]*PaxosGroup{
		"groupA": groupA,
		"groupB": groupB,
	}

	tt := TrueTime{uncertainty: 5 * time.Millisecond}
	client := NewClient(groups, tt)

	// Pre-fill some data
	err := client.RunTransaction(func(tx *Transaction) {
		tx.BufferWrite("balance:A", "100", "groupA")
		tx.BufferWrite("balance:B", "50", "groupB")
	})
	if err != nil {
		fmt.Printf("Initial setup failed: %v\n", err)
		return
	}
	
	// Force an abort by manually acquiring a conflicting lock
	fmt.Println("Acquiring a blocking lock on balance:B in groupB to force abort...")
	groups["groupB"].lockMgr.Acquire("balance:B", WRITE, "blocking-txn", time.Now().UnixNano()-1000000000, make(chan error, 1))

	fmt.Println("Attempting cross-group transaction (should fail/abort)...")
	err = client.RunTransaction(func(tx *Transaction) {
		tx.BufferWrite("balance:A", "90", "groupA")
		tx.BufferWrite("balance:B", "60", "groupB")
	})

	if err != nil {
		fmt.Printf("Transaction aborted as expected: %v\n", err)
	} else {
		fmt.Println("UNEXPECTED: Transaction succeeded!")
	}

	valA, _ := client.Read("balance:A", "groupA")
	valB, _ := client.Read("balance:B", "groupB")

	fmt.Printf("Expected: balance:A = 100, balance:B = 50\n")
	fmt.Printf("Result: balance:A = %s, balance:B = %s\n", valA, valB)
}

func test4() {
	fmt.Println("--- Test 4: External consistency ---")
	nodeList := &NodeList{list: make(map[string]*Node)}
	node1 := NewNode("node1", []string{}, nodeList)
	
	groupA := NewPaxosGroup("groupA", node1)

	groups := map[string]*PaxosGroup{
		"groupA": groupA,
	}

	tt := TrueTime{uncertainty: 5 * time.Millisecond}
	client := NewClient(groups, tt)

	fmt.Println("Txn 1: Writing ext:1 = First")
	fmt.Println("Txn 1: Writing ext:1 = First")
	client.RunTransaction(func(tx *Transaction) {
		tx.BufferWrite("ext:1", "First", "groupA")
	})

	fmt.Println("Waiting for TrueTime uncertainty window...")
	time.Sleep(15 * time.Millisecond)

	fmt.Println("Txn 2: Writing ext:2 = Second")
	client.RunTransaction(func(tx *Transaction) {
		tx.BufferWrite("ext:2", "Second", "groupA")
	})

	storedTS1 := groupA.store.LastTS("ext:1")
	storedTS2 := groupA.store.LastTS("ext:2")

	fmt.Printf("Txn 1 Commit TS: %d\n", storedTS1)
	fmt.Printf("Txn 2 Commit TS: %d\n", storedTS2)

	if storedTS2 > storedTS1 {
		fmt.Println("SUCCESS: External consistency holds (Txn 2 TS > Txn 1 TS)")
	} else {
		fmt.Println("FAILURE: External consistency violated!")
	}
}

func test6() {
	fmt.Println("--- Test 6: Concurrent transactions wound-wait ---")
	nodeList := &NodeList{list: make(map[string]*Node)}
	node1 := NewNode("node1", []string{}, nodeList)

	groupA := NewPaxosGroup("groupA", node1)

	groups := map[string]*PaxosGroup{
		"groupA": groupA,
	}

	tt := TrueTime{uncertainty: 5 * time.Millisecond}
	client := NewClient(groups, tt)

	fmt.Println("Setting initial balances...")
	client.RunTransaction(func(tx *Transaction) {
		tx.BufferWrite("acct:1", "100", "groupA")
	})

	fmt.Println("Starting TxnA (older) and TxnB (younger) concurrently...")
	
	var wg sync.WaitGroup
	wg.Add(2)
	
	errCh := make(chan error, 2)
	
	// Txn A starts...
	go func() {
		defer wg.Done()
		
		// Txn A is older.
		txA_ID := "txnA-older"
		tsA := time.Now().UnixNano()
		
		fmt.Println("TxnA (older): acquiring lock on acct:1")
		err := groups["groupA"].lockMgr.Acquire("acct:1", WRITE, txA_ID, tsA, make(chan error, 1))
		if err != nil {
			errCh <- fmt.Errorf("TxnA error: %v", err)
			return
		}
		
		// Hold the lock for a bit to ensure Txn B tries to get it
		time.Sleep(150 * time.Millisecond)
		
		groups["groupA"].store.Write("acct:1", "150", tsA)
		groups["groupA"].oracle.AdvancePaxosSafe(tsA)
		groups["groupA"].lockMgr.Release(txA_ID)
		
		fmt.Println("TxnA committed successfully.")
	}()

	time.Sleep(20 * time.Millisecond) // Ensure Txn A gets the lock first

	// Txn B starts...
	go func() {
		defer wg.Done()
		
		// Txn B is younger (larger timestamp)
		txB_ID := "txnB-younger"
		tsB := time.Now().UnixNano() + 1000000000
		
		fmt.Println("TxnB (younger): trying to acquire lock on acct:1")
		err := groups["groupA"].lockMgr.Acquire("acct:1", WRITE, txB_ID, tsB, make(chan error, 1))
		
		if err != nil {
			errCh <- fmt.Errorf("TxnB wounded/aborted as expected: %v", err)
		} else {
			// If it somehow succeeds, it shouldn't
			groups["groupA"].store.Write("acct:1", "200", tsB)
			groups["groupA"].lockMgr.Release(txB_ID)
			errCh <- fmt.Errorf("TxnB unexpectedly succeeded!")
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		fmt.Println(err)
	}

	val, _ := client.Read("acct:1", "groupA")
	fmt.Printf("Expected: acct:1 = 150 (TxnA won)\n")
	fmt.Printf("Result: acct:1 = %s\n", val)
}


