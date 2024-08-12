package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

type Provider struct {
	dataDir string
	fileNum map[int]int
}

type FileType int

const (
	FileTypeUnknown FileType = iota
	fileTypeSSTable
)

type FileMetadata struct {
	fileNum  int
	name     string
	level    int
	fileType FileType
	path     string
}

func (f *FileMetadata) IsSSTable() bool {
	return f.fileType == fileTypeSSTable
}

func (f *FileMetadata) FileNum() int {
	return f.fileNum
}

func (f *FileMetadata) Level() int {
	return f.level
}

func (f *FileMetadata) Name() string {
	return f.name
}

func (f *FileMetadata) Path() string {
	return f.path
}

func NewProvider(dataDir string) (*Provider, error) {
	s := &Provider{dataDir: dataDir, fileNum: make(map[int]int)}
	err := s.ensureDataDirExists()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Provider) ensureDataDirExists() error {
	err := os.MkdirAll(s.dataDir, 0755)
	if err != nil {
		return err
	}
	return nil
}

// directory + "/" + level (single digit) + _ + file number (6 digits) + .sst
func (s *Provider) ListFiles() ([]*FileMetadata, error) {
	files, err := os.ReadDir(s.dataDir)
	if err != nil {
		return nil, err
	}
	var meta []*FileMetadata
	var fileNumber int
	var fileLevel int
	var fileExtension string

	for _, f := range files {
		_, err = fmt.Sscanf(f.Name(), "%1d_%06d.%s", &fileLevel, &fileNumber, &fileExtension)
		if err != nil {
			return nil, err
		}
		if fileExtension != "sst" {
			continue
		}
		meta = append(meta, &FileMetadata{
			fileNum:  fileNumber,
			level:    fileLevel,
			fileType: fileTypeSSTable,
			name:     f.Name(),
			path:     filepath.Join(s.dataDir, f.Name()),
		})
		// 각 레벨의 최대 파일 번호 업데이트
		if currentMax, exists := s.fileNum[fileLevel]; !exists || fileNumber > currentMax {
			s.fileNum[fileLevel] = fileNumber
		}
	}
	return meta, nil
}

func (s *Provider) nextFileNum(level int) int {
	s.fileNum[level]++
	return s.fileNum[level]
}

func (s *Provider) generateFileName(level, seq int) string {
	return fmt.Sprintf("%1d_%06d.sst", level, seq)
}

func (s *Provider) PrepareNewFile(level int) *FileMetadata {
	return &FileMetadata{
		fileNum:  s.nextFileNum(level),
		level:    level,
		fileType: fileTypeSSTable,
	}
}

func (s *Provider) OpenFileForWriting(meta *FileMetadata) (*os.File, error) {
	const openFlags = os.O_RDWR | os.O_CREATE | os.O_EXCL
	filename := s.generateFileName(meta.level, meta.fileNum)
	file, err := os.OpenFile(filepath.Join(s.dataDir, filename), openFlags, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (s *Provider) OpenFileForReading(meta *FileMetadata) (*os.File, error) {
	const openFlags = os.O_RDONLY
	filename := s.generateFileName(meta.level, meta.fileNum)
	file, err := os.OpenFile(filepath.Join(s.dataDir, filename), openFlags, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}
