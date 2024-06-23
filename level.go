package lsm

import "math"

const (
	maxLevel   = 7
	l0Capacity = 5 // 5개 생기면 l0 compaction
	growFactor = 10
)

type level struct {
	sstables []*SSTable // SSTables in this level
}

func (l *level) sstableToCompact() *SSTable {
	// 현재 레벨에서 가장 오래된 파일 반환
	if len(l.sstables) == 0 {
		return nil
	}
	minSSTable := l.sstables[0]
	for _, sstable := range l.sstables {
		if sstable.file.Name() < minSSTable.file.Name() {
			minSSTable = sstable
		}
	}
	return minSSTable
}

func (l *level) TotalSize() int {
	totalSize := 0
	for _, sstable := range l.sstables {
		f, _ := sstable.file.Stat()
		totalSize += int(f.Size())
	}
	return totalSize
}

func calculateLevelSize(level int) int {
	return memtableSizeLimitBytes * l0Capacity * int(math.Pow(float64(growFactor), float64(level)))
}

func calculateMaxFileSize(level int) int {
	return calculateLevelSize(level - 1)
}
