package main

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type cfg struct {
	host      string
	port      int
	clients   int
	duration  time.Duration
	pipeline  int
	keyspace  int
	valueSize int
	mode      string
	ratioGet  float64
	timeout   time.Duration
}

type workerResult struct {
	ops   uint64
	bytes uint64
	batch []time.Duration
	err   error
}

func main() {
	c := cfg{}
	flag.StringVar(&c.host, "host", "127.0.0.1", "")
	flag.IntVar(&c.port, "port", 16379, "")
	flag.IntVar(&c.clients, "clients", 50, "")
	flag.DurationVar(&c.duration, "duration", 20*time.Second, "")
	flag.IntVar(&c.pipeline, "pipeline", 16, "")
	flag.IntVar(&c.keyspace, "keyspace", 100000, "")
	flag.IntVar(&c.valueSize, "value_size", 256, "")
	flag.StringVar(&c.mode, "mode", "mixed", "")
	flag.Float64Var(&c.ratioGet, "ratio_get", 0.8, "")
	flag.DurationVar(&c.timeout, "timeout", 2*time.Second, "")
	flag.Parse()

	if c.clients <= 0 {
		c.clients = 1
	}
	if c.pipeline <= 0 {
		c.pipeline = 1
	}
	if c.keyspace <= 0 {
		c.keyspace = 1
	}
	if c.valueSize < 0 {
		c.valueSize = 0
	}
	switch c.mode {
	case "set", "get", "mixed":
	default:
		fmt.Fprintln(os.Stderr, "mode must be set|get|mixed")
		os.Exit(2)
	}
	if c.ratioGet < 0 {
		c.ratioGet = 0
	}
	if c.ratioGet > 1 {
		c.ratioGet = 1
	}

	addr := net.JoinHostPort(c.host, strconv.Itoa(c.port))
	value := makeValue(c.valueSize)

	prefillKeys := min(c.keyspace, 20000)
	if c.mode != "get" && prefillKeys > 0 {
		if err := prefill(addr, c.timeout, prefillKeys, value); err != nil {
			fmt.Fprintln(os.Stderr, "prefill:", err)
			os.Exit(1)
		}
	}
	if c.mode == "get" && prefillKeys > 0 {
		if err := prefill(addr, c.timeout, prefillKeys, value); err != nil {
			fmt.Fprintln(os.Stderr, "prefill:", err)
			os.Exit(1)
		}
	}

	deadline := time.Now().Add(c.duration)

	var totalOps uint64
	var totalBytes uint64
	results := make([]workerResult, c.clients)
	var wg sync.WaitGroup
	wg.Add(c.clients)

	started := time.Now()
	for i := 0; i < c.clients; i++ {
		i := i
		go func() {
			defer wg.Done()
			r := runWorker(addr, c, value, deadline, int64(i))
			results[i] = r
			atomic.AddUint64(&totalOps, r.ops)
			atomic.AddUint64(&totalBytes, r.bytes)
		}()
	}

	wg.Wait()
	elapsed := time.Since(started)

	var allBatches []time.Duration
	for _, r := range results {
		if r.err != nil {
			fmt.Fprintln(os.Stderr, "worker error:", r.err)
		}
		allBatches = append(allBatches, r.batch...)
	}

	opsPerSec := float64(totalOps) / elapsed.Seconds()
	mbPerSec := float64(totalBytes) / (1024 * 1024) / elapsed.Seconds()

	latP50, latP95, latP99 := batchLatencyStats(allBatches, c.pipeline)

	fmt.Printf("target=%s mode=%s clients=%d pipeline=%d duration=%s keyspace=%d value_size=%d\n",
		addr, c.mode, c.clients, c.pipeline, c.duration, c.keyspace, c.valueSize)
	fmt.Printf("ops_total=%d ops_per_sec=%.0f mb_per_sec=%.2f\n", totalOps, opsPerSec, mbPerSec)
	fmt.Printf("latency_per_op_ms p50=%.3f p95=%.3f p99=%.3f (batch-derived)\n",
		latP50.Seconds()*1000, latP95.Seconds()*1000, latP99.Seconds()*1000)
	fmt.Printf("runtime_go_version=%s cpu=%d\n", runtime.Version(), runtime.NumCPU())
}

func prefill(addr string, timeout time.Duration, n int, value []byte) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	br := bufio.NewReaderSize(conn, 1<<20)
	bw := bufio.NewWriterSize(conn, 1<<20)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("bench:%d", i)
		writeArrayHeader(bw, 3)
		writeBulkString(bw, "SET")
		writeBulkString(bw, key)
		writeBulkBytes(bw, value)
		if err := bw.Flush(); err != nil {
			return err
		}
		if err := readRESP(br); err != nil {
			return err
		}
	}
	return nil
}

func runWorker(addr string, c cfg, value []byte, deadline time.Time, seed int64) workerResult {
	conn, err := net.DialTimeout("tcp", addr, c.timeout)
	if err != nil {
		return workerResult{err: err}
	}
	defer conn.Close()

	br := bufio.NewReaderSize(conn, 1<<20)
	bw := bufio.NewWriterSize(conn, 1<<20)
	r := mrand.New(mrand.NewSource(seed ^ time.Now().UnixNano()))

	batchDur := make([]time.Duration, 0, int(c.duration.Seconds())*10)
	var ops uint64
	var bytes uint64

	for time.Now().Before(deadline) {
		start := time.Now()
		n := c.pipeline
		for i := 0; i < n; i++ {
			k := r.Intn(c.keyspace)
			key := fmt.Sprintf("bench:%d", k)

			switch c.mode {
			case "set":
				writeArrayHeader(bw, 3)
				writeBulkString(bw, "SET")
				writeBulkString(bw, key)
				writeBulkBytes(bw, value)
				bytes += uint64(len(key) + len(value) + 32)
			case "get":
				writeArrayHeader(bw, 2)
				writeBulkString(bw, "GET")
				writeBulkString(bw, key)
				bytes += uint64(len(key) + 16)
			default:
				if r.Float64() < c.ratioGet {
					writeArrayHeader(bw, 2)
					writeBulkString(bw, "GET")
					writeBulkString(bw, key)
					bytes += uint64(len(key) + 16)
				} else {
					writeArrayHeader(bw, 3)
					writeBulkString(bw, "SET")
					writeBulkString(bw, key)
					writeBulkBytes(bw, value)
					bytes += uint64(len(key) + len(value) + 32)
				}
			}
		}

		if err := bw.Flush(); err != nil {
			return workerResult{ops: ops, bytes: bytes, batch: batchDur, err: err}
		}
		for i := 0; i < n; i++ {
			if err := readRESP(br); err != nil {
				return workerResult{ops: ops, bytes: bytes, batch: batchDur, err: err}
			}
		}
		d := time.Since(start)
		batchDur = append(batchDur, d)
		ops += uint64(n)
	}

	return workerResult{ops: ops, bytes: bytes, batch: batchDur}
}

func batchLatencyStats(batch []time.Duration, pipeline int) (time.Duration, time.Duration, time.Duration) {
	if len(batch) == 0 || pipeline <= 0 {
		return 0, 0, 0
	}
	perOp := make([]time.Duration, 0, len(batch))
	div := float64(pipeline)
	for _, d := range batch {
		per := time.Duration(float64(d) / div)
		perOp = append(perOp, per)
	}
	sort.Slice(perOp, func(i, j int) bool { return perOp[i] < perOp[j] })
	p50 := perOp[int(math.Round(0.50*float64(len(perOp)-1)))]
	p95 := perOp[int(math.Round(0.95*float64(len(perOp)-1)))]
	p99 := perOp[int(math.Round(0.99*float64(len(perOp)-1)))]
	return p50, p95, p99
}

func makeValue(n int) []byte {
	if n == 0 {
		return nil
	}
	b := make([]byte, n)
	_, _ = io.ReadFull(rand.Reader, b)
	for i := range b {
		b[i] = 'a' + (b[i] % 26)
	}
	return b
}

func writeArrayHeader(w *bufio.Writer, n int) {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(n))
	w.WriteString("\r\n")
}

func writeBulkString(w *bufio.Writer, s string) {
	writeBulkBytes(w, []byte(s))
}

func writeBulkBytes(w *bufio.Writer, b []byte) {
	w.WriteByte('$')
	w.WriteString(strconv.Itoa(len(b)))
	w.WriteString("\r\n")
	if len(b) > 0 {
		w.Write(b)
	}
	w.WriteString("\r\n")
}

func readRESP(r *bufio.Reader) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	switch b {
	case '+', '-', ':':
		_, err := readLine(r)
		return err
	case '$':
		line, err := readLine(r)
		if err != nil {
			return err
		}
		n, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			return err
		}
		if n < 0 {
			return nil
		}
		if _, err := io.CopyN(io.Discard, r, int64(n+2)); err != nil {
			return err
		}
		return nil
	case '*':
		line, err := readLine(r)
		if err != nil {
			return err
		}
		n, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := readRESP(r); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("invalid resp type byte: %q", b)
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("invalid line")
	}
	return line[:len(line)-2], nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() {
	var seed int64
	_ = binary.Read(rand.Reader, binary.LittleEndian, &seed)
	mrand.Seed(seed)
}
