package surfstore

import (
	context "context"
	"crypto/sha256"
    "encoding/hex"
    "fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, ok := bs.BlockMap[blockHash.GetHash()]

    if !ok {
    	return &Block{}, fmt.Errorf("Hash is not found in the map")
    } else {
    	return block, nil
    }
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hashBytes := sha256.Sum256(block.GetBlockData())
    hashString := hex.EncodeToString(hashBytes[:])
    bs.BlockMap[hashString] = block
    return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashListOut []string
	hashListIn := blockHashesIn.GetHashes()
	for i := 0; i < len(hashListIn); i++ {
	    _, ok := bs.BlockMap[hashListIn[i]]
	    if ok {
	        hashListOut = append(hashListOut,hashListIn[i])
	    }
	}
	return &BlockHashes{Hashes: hashListOut}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
