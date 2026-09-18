package worker

import (
	"net"
	"sync/atomic"
	"time"
)

// TrafficStats menyimpan jumlah byte masuk dan keluar
// menggunakan operasi atomik.
type TrafficStats struct {
	RxBytes uint64 // Receiver / Download
	TxBytes uint64 // Transmitter / Upload
}

// TrafficSnapshot adalah snapshot counter pada satu waktu.
type TrafficSnapshot struct {
	RxBytes uint64
	TxBytes uint64
	Time    time.Time
}

// TrafficRate menyimpan estimasi traffic per detik.
type TrafficRate struct {
	RxBytesPerSec float64
	TxBytesPerSec float64
}

// ObservedConn membungkus net.Conn standar untuk
// menghitung setiap byte yang lewat.
type ObservedConn struct {
	net.Conn
	stats *TrafficStats
}

// NewObservedConn menginisialisasi pembungkus koneksi
// untuk tracking statistik.
func NewObservedConn(
	conn net.Conn,
	stats *TrafficStats,
) net.Conn {
	return &ObservedConn{
		Conn:  conn,
		stats: stats,
	}
}

// Read mengintersep data masuk (Download / Rx).
func (o *ObservedConn) Read(
	b []byte,
) (n int, err error) {

	n, err = o.Conn.Read(b)

	if n > 0 && o.stats != nil {
		atomic.AddUint64(
			&o.stats.RxBytes,
			uint64(n),
		)
	}

	return n, err
}

// Write mengintersep data keluar (Upload / Tx).
func (o *ObservedConn) Write(
	b []byte,
) (n int, err error) {

	n, err = o.Conn.Write(b)

	if n > 0 && o.stats != nil {
		atomic.AddUint64(
			&o.stats.TxBytes,
			uint64(n),
		)
	}

	return n, err
}

// GetStats mengembalikan jumlah Rx dan Tx saat ini.
func (o *ObservedConn) GetStats() (uint64, uint64) {
	if o.stats == nil {
		return 0, 0
	}

	return atomic.LoadUint64(&o.stats.RxBytes),
		atomic.LoadUint64(&o.stats.TxBytes)
}

// Snapshot mengambil snapshot counter traffic.
//
// Fungsi ini aman dipanggil dari goroutine lain karena
// menggunakan atomic.LoadUint64.
func (s *TrafficStats) Snapshot() TrafficSnapshot {
	if s == nil {
		return TrafficSnapshot{
			Time: time.Now(),
		}
	}

	return TrafficSnapshot{
		RxBytes: atomic.LoadUint64(&s.RxBytes),
		TxBytes: atomic.LoadUint64(&s.TxBytes),
		Time:    time.Now(),
	}
}

// Rate menghitung perubahan traffic sejak snapshot sebelumnya.
func (s *TrafficStats) Rate(
	previous TrafficSnapshot,
) (TrafficRate, TrafficSnapshot) {

	current := s.Snapshot()

	elapsed := current.Time.Sub(previous.Time)

	if elapsed <= 0 {
		return TrafficRate{}, current
	}

	seconds := elapsed.Seconds()

	rxDelta := uint64(0)
	txDelta := uint64(0)

	if current.RxBytes >= previous.RxBytes {
		rxDelta = current.RxBytes - previous.RxBytes
	}

	if current.TxBytes >= previous.TxBytes {
		txDelta = current.TxBytes - previous.TxBytes
	}

	return TrafficRate{
		RxBytesPerSec: float64(rxDelta) / seconds,
		TxBytesPerSec: float64(txDelta) / seconds,
	}, current
}