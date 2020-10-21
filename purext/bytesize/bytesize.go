package bytesize

import "fmt"

type (
	Byte int
)

const (
	B Byte = 1 << (10 * iota)
	KB
	MB
	GB
	TB
	PB
)

func (b Byte) Int() int {
	return int(b)
}

func (b Byte) Int64() int64 {
	return int64(b)
}

func (b Byte) KB() float64 {
	return float64(b) / float64(KB)
}

func (b Byte) MB() float64 {
	return float64(b) / float64(MB)
}

func (b Byte) GB() float64 {
	return float64(b) / float64(GB)
}

func (b Byte) TB() float64 {
	return float64(b) / float64(TB)
}

func (b Byte) PB() float64 {
	return float64(b) / float64(PB)
}

func (b Byte) KBString() string {
	return fmt.Sprintf("%.2fKB", b.KB())
}

func (b Byte) MBString() string {
	return fmt.Sprintf("%.2fMB", b.MB())
}

func (b Byte) GBString() string {
	return fmt.Sprintf("%.2fGB", b.GB())
}

func (b Byte) TBString() string {
	return fmt.Sprintf("%.2fTB", b.TB())
}

func (b Byte) PBString() string {
	return fmt.Sprintf("%.2fPB", b.PB())
}
