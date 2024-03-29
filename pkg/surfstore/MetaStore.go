package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"

)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {

	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {

	currFilename := fileMetaData.Filename
	pastFileMetaData,ok := m.FileMetaMap[currFilename]
	//fmt.Println("past:",pastFileMetaData.GetVersion())
	//fmt.Println("curr:",fileMetaData.GetVersion())
	if ok && pastFileMetaData.GetVersion() != fileMetaData.GetVersion() - 1 {
	    var ret int32 = -1
	    return &Version{Version: ret},  nil
	} else {
	    m.FileMetaMap[currFilename] = fileMetaData
        return &Version{Version: fileMetaData.GetVersion()}, nil
	}

}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {

	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
