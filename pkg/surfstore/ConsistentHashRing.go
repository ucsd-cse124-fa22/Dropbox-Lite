package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) InsertServer(addr string) {
	key := c.Hash(addr)
	c.ServerMap[key] = strings.ReplaceAll(addr,"blockstore","")
}

func (c ConsistentHashRing) DeleteServer(addr string) {
	key := c.Hash(addr)
	delete(c.ServerMap,key)
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// Find the next largest key from ServerMap
	blockIdHash := c.Hash(blockId)
	var hashKeyArr []string
	for hashKey,_ := range c.ServerMap {
	    hashKeyArr = append(hashKeyArr,hashKey)
	}
	sort.Strings(hashKeyArr)

	for _,hashKey := range hashKeyArr {
	    if hashKey > blockIdHash {
	        return c.ServerMap[hashKey]
	    }
	}
	return c.ServerMap[hashKeyArr[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func (c ConsistentHashRing) OutputMap(blockHashes []string) map[string]string {
	res := make(map[string]string)
	for i := 0; i < len(blockHashes); i++ {
		res["block"+strconv.Itoa(i)] = c.GetResponsibleServer(blockHashes[i])
	}
	return res
}

func NewConsistentHashRing(numServers int, downServer []int) *ConsistentHashRing {
	c := &ConsistentHashRing{
		ServerMap: make(map[string]string),
	}

	for i := 0; i < numServers; i++ {
		c.InsertServer("blockstore" + strconv.Itoa(i))
	}

	for i := 0; i < len(downServer); i++ {
		c.DeleteServer("blockstore" + strconv.Itoa(downServer[i]))
	}

	return c
}
