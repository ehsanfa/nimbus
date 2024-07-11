package storage

type entry struct {
	key       string
	value     string
	timestamp int64
}

type CommitLog struct {
	entries []entry
}

func (c *CommitLog) Commit(k, v string, timestamp int64) {
	c.entries = append(c.entries, entry{k, v, timestamp})
}

func NewCommitLog() CommitLog {
	return CommitLog{entries: make([]entry, 100)}
}
