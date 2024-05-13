package main

import (
	"fmt"
	m "math"
	"os"
	"strconv"
)

func test() {
	fmt.Println("Hello, World!")

	beyondHello()
}

func beyondHello() {
	var x int
	x = 3
	y := 4
	sum, prod := learnMuliple(x, y)
	fmt.Println("sum:", sum, "prod:", prod)

	learnTypes()
}

func learnMuliple(x, y int) (sum, prod int) {
	return x + y, x * y
}

func learnTypes() {
	str := "Learn Go!"
	s2 := `A "raw" string literal`
	g := 'Σ'

	f := 3.14195
	c := 3 + 4i

	var u uint = 7
	var pi float32 = 22. / 7

	n := byte('\n')

	var a4 [4]int
	a5 := [...]int{3, 1, 5, 10, 100}
	fmt.Println(a5)

	a4_cpy := a4   // a copy of a4, two separate arrays
	a4_cpy[0] = 25 // only the copy is changed, not a4
	fmt.Println(a4_cpy[0] == a4[0])

	s3 := []int{4, 5, 9}
	s4 := make([]int, 4)
	fmt.Println(s3, s4)

	var d2 [][]float64
	bs := []byte("a slice")

	s3_cpy := s3 // both s3 and s3_cpy point to the same array
	s3_cpy[0] = 0
	fmt.Println(s3_cpy[0] == s3[0])

	s := []int{1, 2, 3}
	s = append(s, 4, 5, 6)
	fmt.Println(s)

	s = append(s, []int{7, 8, 9}...)
	fmt.Println(s)

	p, q := learnMemory()
	fmt.Println(*p, *q)

	m := map[string]int{"three": 3, "four": 4}
	m["one"] = 1

	_, _, _, _, _, _, _, _, _, _ = str, s2, g, f, c, u, pi, n, a4, bs

	file, _ := os.Create("output.txt")
	fmt.Fprint(file, "This is how you write to a file, by the wway")
	file.Close()

	fmt.Println(s, c, a4, s3, d2, m)

	learnFlowControl()

}

func learnMemory() (p, q *int) {
	p = new(int)
	s := make([]int, 20)
	s[3] = 7
	r := -2
	return &s[3], &r
}

func learnNamedReturns(x, y int) (z int) {
	z = x + y
	return
}

func expensiveComputation() float64 {
	return m.Exp(10)
}

func learnFlowControl() {
	if true {
		fmt.Println("told ya")
	}
	if false {
		// pout
	} else {
		// gloat
	}
	x := 42.0
	switch x {
	case 0:
	case 1, 2:
	case 42:
	case 43:
	default:
	}

	var data interface{}

	data = ""
	switch c := data.(type) {
	case string:
		fmt.Println(c, "is a string")
	case int64:
		fmt.Printf("%d is an int64\n", c)
	default:

	}

	for x := 0; x < 3; x++ {
		fmt.Println("iteration", x)
	}

	for {
		break
		continue
	}

	for key, value := range map[string]int{"one": 1, "two": 2, "three": 3} {
		fmt.Printf("key=%s, value=%d\n", key, value)
	}
	for _, name := range []string{"Bob", "Bill", "Joe"} {
		fmt.Printf("Hello, %s\n", name)
	}

	if y := expensiveComputation(); y > x {
		x = y
	}

	xBig := func() bool {
		return x > 10000
	}
	x = 99999
	fmt.Println("xBig:", xBig())
	x = 1.3e3
	fmt.Println("xBig:", xBig())

	fmt.Println("Add + double two numbers: ", func(a, b int) int {
		return (a + b) * 2
	}(10, 2))

	goto love
love:
	learnFunctionFactory()
	learnDefer()
	learnInterfaces()
}

func learnFunctionFactory() {
	fmt.Println(sentenceFactory("summer")("A beautiful", "day!"))

	d := sentenceFactory("summer")
	fmt.Println(d("A beautiful", "day!"))
	fmt.Println(d("A lazy", "afternoon!"))
}

func sentenceFactory(mystring string) func(before, after string) string {
	return func(before, after string) string {
		return fmt.Sprintf("%s %s %s", before, mystring, after)
	}
}

func learnDefer() (ok bool) {
	fmt.Println("how about this ?")
	defer fmt.Println("deferred statements execute in reverse (LIFO) order.")
	defer fmt.Println("This line is being printed first because")
	return true
}

type Stringer interface {
	String() string
}

type pair struct {
	x, y int
}

func (p pair) String() string {
	return fmt.Sprintf("(%d, %d)", p.x, p.y)
}

func learnInterfaces() {
	p := pair{3, 4}
	fmt.Println(p.String())
	var i Stringer
	i = p
	fmt.Println(i.String())

	fmt.Println(p)
	fmt.Println(i)

	learnVariadicParams("great", "learning", "here!")
}

func learnVariadicParams(myStrings ...interface{}) {
	for _, param := range myStrings {
		fmt.Println("param:", param)
	}
	fmt.Println("parram:", fmt.Sprintln(myStrings...))

	learnErrorHandling()
}

func learnErrorHandling() {
	m := map[int]string{3: "three", 4: "four"}
	if x, ok := m[1]; !ok {
		fmt.Println("no one there")
	} else {
		fmt.Print(x)
	}

	if _, err := strconv.Atoi("non-int"); err != nil {
		fmt.Println(err)
	}

	learnConcurrency()
}

func inc(i int, c chan int) {
	c <- i + 1
}

func learnConcurrency() {
	c := make(chan int)

	go inc(0, c)
	go inc(10, c)
	go inc(-805, c)

	fmt.Println(<-c, <-c, <-c)

	cs := make(chan string)
	ccs := make(chan chan string)

	go func() { c <- 84 }()
	go func() { cs <- "wordy" }()

	select {
	case i := <-c:
		fmt.Printf("it's a %T", i)
	case <-cs:
		fmt.Printf("it's a String")
	case <-ccs:
		fmt.Println("didn't happen")
	}
}
