package utils

import (
	"strconv"
	"testing"
)

func BenchmarkGeneratePartition(b *testing.B) {
	const key = "tenant-123:orders:987654321"
	for _, partitions := range []int{4, 16, 64, 256} {
		b.Run(strconv.Itoa(partitions), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = GeneratePartition(key, partitions)
			}
		})
	}
}

func BenchmarkCombineKeys(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = CombineKeys("tenant-123", "orders", "987654321")
	}
}
