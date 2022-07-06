package shardkv

import "raftkv/src/shardmaster"

type StoreComponent struct {
	data [shardmaster.NShards]map[string]string
}

func MakeStore() *StoreComponent {
	data := [shardmaster.NShards]map[string]string{}
	for i := 0; i < shardmaster.NShards; i++ {
		data[i] = make(map[string]string)
	}
	return &StoreComponent{
		data: data,
	}
}

func (db *StoreComponent) UpdateDB(op int, key string, value string) (string, Err) {
	var status Err = OK
	shard := key2shard(key)
	switch op {
	case OpGet:
		if _, ok := db.data[shard][key]; !ok {
			status = ErrNoKey
		} else {
			value = db.data[shard][key]
		}
	case OpPut:
		db.data[shard][key] = value
		value = db.data[shard][key]
	case OpAppend:
		db.data[shard][key] += value
		value = db.data[shard][key]
	}
	return value, status
}

func (db *StoreComponent) UpdateFrom(data [shardmaster.NShards]map[string]string) {
	for shard, kv := range data {
		for k, v := range kv {
			db.data[shard][k] = v
		}
	}
}

func (db *StoreComponent) CopyFrom(shards []int) [shardmaster.NShards]map[string]string {
	data := [shardmaster.NShards]map[string]string{}
	for i := 0; i < shardmaster.NShards; i++ {
		data[i] = make(map[string]string)
	}
	for _, shard := range shards {
		data[shard] = DeepCopy(db.data[shard])
	}
	return data
}

func (db *StoreComponent) DeleteShard(shard int) {
	db.data[shard] = make(map[string]string)
}
