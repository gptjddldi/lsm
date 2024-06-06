package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

type Provider struct {
	dataDir string
	fileNum int
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

func NewProvider(dataDir string) (*Provider, error) {
	s := &Provider{dataDir: dataDir}
	fmt.Println("Test")
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
		})
		s.nextFileNum()
	}
	return meta, nil
}

func (s *Provider) nextFileNum() int {
	s.fileNum++
	return s.fileNum
}

func (s *Provider) generateFileName(fileNumber int) string {
	return fmt.Sprintf("%06d.sst", fileNumber)
}

func (s *Provider) PrepareNewFile() *FileMetadata {
	return &FileMetadata{
		fileNum:  s.nextFileNum(),
		fileType: fileTypeSSTable,
	}
}

func (s *Provider) OpenFileForWriting(meta *FileMetadata) (*os.File, error) {
	const openFlags = os.O_RDWR | os.O_CREATE | os.O_EXCL
	filename := s.generateFileName(meta.fileNum)
	file, err := os.OpenFile(filepath.Join(s.dataDir, filename), openFlags, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (s *Provider) OpenFileForReading(meta *FileMetadata) (*os.File, error) {
	const openFlags = os.O_RDONLY
	filename := s.generateFileName(meta.fileNum)
	file, err := os.OpenFile(filepath.Join(s.dataDir, filename), openFlags, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}
