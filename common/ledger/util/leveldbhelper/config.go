/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package leveldbhelper

import (
	"github.com/spf13/viper"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// LevelDBDef contains parameters
type LevelDBDef struct {
	// CompactionL0Trigger defines number of 'sorted table' at level-0 that will
	// trigger compaction.
	CompactionL0Trigger int
	// WriteL0StopTrigger defines number of 'sorted table' at level-0 that will
	// pause write.
	WriteL0PauseTrigger int
	// WriteL0SlowdownTrigger defines number of 'sorted table' at level-0 that
	// will trigger write slowdown.
	WriteL0SlowdownTrigger int
	// WriteBuffer defines maximum size of a 'memdb' before flushed to
	// 'sorted table'. 'memdb' is an in-memory DB backed by an on-disk
	// unsorted journal.
	WriteBuffer int
	// IteratorSamplingRate defines approximate gap (in bytes) between read
	// sampling of an iterator. The samples will be used to determine when
	// compaction should be triggered.
	IteratorSamplingRate int
	// CompactionTotalSize limits total size of 'sorted table' for each level.
	// The limits for each level will be calculated as:
	//   CompactionTotalSize * (CompactionTotalSizeMultiplier ^ Level)
	// The multiplier for each level can also fine-tuned using
	// CompactionTotalSizeMultiplierPerLevel.
	CompactionTotalSize int
	// CompactionTotalSizeMultiplier defines multiplier for CompactionTotalSize.
	CompactionTotalSizeMultiplier float64
	// CompactionTableSize limits size of 'sorted table' that compaction generates.
	// The limits for each level will be calculated as:
	//   CompactionTableSize * (CompactionTableSizeMultiplier ^ Level)
	// The multiplier for each level can also fine-tuned using CompactionTableSizeMultiplierPerLevel.
	CompactionTableSize int
	// CompactionTableSizeMultiplier defines multiplier for CompactionTableSize.
	CompactionTableSizeMultiplier float64
}

//GetLevelDBDefinition exposes the LevelDB variable
func GetLevelDBDefinition() *LevelDBDef {
	compactionL0Trigger := viper.GetInt("ledger.state.levelDBConfig.compactionL0Trigger")
	writeL0PauseTrigger := viper.GetInt("ledger.state.levelDBConfig.writeL0PauseTrigger")
	writeL0SlowdownTrigger := viper.GetInt("ledger.state.levelDBConfig.writeL0SlowdownTrigger")
	writeBuffer := viper.GetInt("ledger.state.levelDBConfig.writeBuffer")
	iteratorSamplingRate := viper.GetInt("ledger.state.levelDBConfig.iteratorSamplingRate")
	compactionTotalSize := viper.GetInt("ledger.state.levelDBConfig.compactionTotalSize")
	compactionTotalSizeMultiplier := viper.GetFloat64("ledger.state.levelDBConfig.compactionTotalSizeMultiplier")
	compactionTableSize := viper.GetInt("ledger.state.levelDBConfig.compactionTableSize")
	compactionTableSizeMultiplier := viper.GetFloat64("ledger.state.levelDBConfig.compactionTableSizeMultiplier")

	return &LevelDBDef{compactionL0Trigger, writeL0PauseTrigger,
		writeL0SlowdownTrigger, writeBuffer * opt.MiB,
		iteratorSamplingRate * opt.MiB, compactionTotalSize * opt.MiB,
		compactionTotalSizeMultiplier, compactionTableSize * opt.MiB,
		compactionTableSizeMultiplier}
}
