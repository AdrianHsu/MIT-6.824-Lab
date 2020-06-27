package mapreduce

import "testing"
import "fmt"
import "time"
import "container/list"
import "strings"
import "os"
import "bufio"
import "log"
import "sort"
import "strconv"

const (
	nNumber = 100000
	nMap    = 100
	nReduce = 50
)

// Create input file with N numbers
// Check if we have N numbers in output file

// Split in words
func MapFunc(value string) *list.List {
	DPrintf("Map %v\n", value)
	res := list.New()
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res.PushBack(kv)
	}
	return res
}

// Just return key
func ReduceFunc(key string, values *list.List) string {
	for e := values.Front(); e != nil; e = e.Next() {
		DPrintf("Reduce %s %v\n", key, e.Value)
	}
	return ""
}

// Checks input file against output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, file string) {
	input, err := os.Open(file)
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer input.Close()
	output, err := os.Open("mrtmp." + file)
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	inputScanner := bufio.NewScanner(input)
	for inputScanner.Scan() {
		lines = append(lines, inputScanner.Text())
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i += 1
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 RPC.
func checkWorker(t *testing.T, l *list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// Make input file
func makeInput() string {
	name := "824-mrinput.txt"
	file, err := os.Create(name)
	if err != nil {
		log.Fatal("mkInput: ", err)
	}
	w := bufio.NewWriter(file)
	for i := 0; i < nNumber; i++ {
		fmt.Fprintf(w, "%d\n", i)
	}
	w.Flush()
	file.Close()
	return name
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *MapReduce {
	file := makeInput()
	master := port("master")
	mr := MakeMapReduce(nMap, nReduce, file, master)
	return mr
}

func cleanup(mr *MapReduce) {
	mr.CleanupFiles()
	RemoveFile(mr.file)
}

func TestBasic(t *testing.T) {
	fmt.Printf("Test: Basic mapreduce ...\n")
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1)
	}
	// Wait until MR is done
	<-mr.DoneChannel
	check(t, mr.file)
	checkWorker(t, mr.stats)
	cleanup(mr)
	fmt.Printf("  ... Basic Passed\n")
}

func TestOneFailure(t *testing.T) {
	fmt.Printf("Test: One Failure mapreduce ...\n")
	mr := setup()
	// Start 2 workers that fail after 10 jobs
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(0)),
		MapFunc, ReduceFunc, 10)
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(1)),
		MapFunc, ReduceFunc, -1)
	// Wait until MR is done
	<-mr.DoneChannel
	check(t, mr.file)
	checkWorker(t, mr.stats)
	cleanup(mr)
	fmt.Printf("  ... One Failure Passed\n")
}

func TestManyFailures(t *testing.T) {
	fmt.Printf("Test: One ManyFailures mapreduce ...\n")
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.DoneChannel:
			check(t, mr.file)
			cleanup(mr)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 jobs
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.MasterAddress, w, MapFunc, ReduceFunc, 10)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.MasterAddress, w, MapFunc, ReduceFunc, 10)
			i++
			time.Sleep(1 * time.Second)
		}
	}

	fmt.Printf("  ... Many Failures Passed\n")
}
