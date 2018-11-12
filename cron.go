package cron

import (
	"errors"
	"log"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries  []*Entry
	stop     chan struct{}
	add      chan *Entry
	remove   chan EntryID
	snapshot chan []*Entry
	running  bool
	nextID   EntryID
	ErrorLog Logger
	location *time.Location
}

// EntryID identifies an entry within a Cron instance
type EntryID uint64

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job to run.
	Job Job

	// The Job's name. Useful when querying the schedule
	Name string

	// Spec is a string used to make a schedule
	Spec string
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, in the Local time zone.
func New() *Cron {
	return NewWithLocation(time.Now().Location())
}

// NewWithLocation returns a new Cron job runner.
func NewWithLocation(location *time.Location) *Cron {
	return &Cron{
		entries:  nil,
		add:      make(chan *Entry),
		stop:     make(chan struct{}),
		remove:   make(chan EntryID),
		snapshot: make(chan []*Entry),
		running:  false,
		ErrorLog: nil,
		location: location,
	}
}

// A wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// A wrapper that turns a func() into a cron.Job
type FuncJobComplex struct {
	function interface{}
	params   []interface{}
}

func (el FuncJobComplex) Run() {
	f := reflect.ValueOf(el.function)
	in := make([]reflect.Value, len(el.params))
	for k, param := range el.params {
		in[k] = reflect.ValueOf(param)
	}
	f.Call(in)
}

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd interface{}, params ...interface{}) (EntryID, error) {
	return c.AddNamedFunc(spec, "", cmd, params...)
}

// AddNamedFunc adds a func to the Cron to be run on the given schedule. It
// accepts an extra name parameter used to identify the function among others.
func (c *Cron) AddNamedFunc(spec, name string, cmd interface{}, params ...interface{}) (EntryID, error) {
	f := reflect.ValueOf(cmd)
	if len(params) != f.Type().NumIn() {
		var refP = reflect.ValueOf(cmd).Pointer()
		var refName = runtime.FuncForPC(refP).Name()
		var file, line = runtime.FuncForPC(refP).FileLine(runtime.FuncForPC(refP).Entry())
		log.Printf("the number of param is not adapted for function %v [ line: %v - file: %v ]", refName, line, file)
		return 0, errors.New("the number of param is not adapted for function " + refName + " [ line: " + strconv.FormatInt(int64(line), 10) + " - file: " + file + " ]")
	}
	return c.AddNamedJob(spec, name, FuncJobComplex{cmd, params})
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	return c.AddNamedJob(spec, "", cmd)
}

// AddNamedJob adds a Job to the Cron to be run on the given schedule and a
// given name
func (c *Cron) AddNamedJob(spec, name string, cmd Job) (EntryID, error) {
	schedule, err := Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.ScheduleNamed(schedule, name, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	return c.ScheduleNamed(schedule, "", cmd)
}

var idLock sync.Mutex

func (c *Cron) nextid() EntryID {
	idLock.Lock()
	defer idLock.Unlock()
	c.nextID++
	return c.nextID
}

// ScheduleNamed adds a named Job to the Cron to be run on a given schedule.
func (c *Cron) ScheduleNamed(schedule Schedule, name string, cmd Job) EntryID {
	entry := &Entry{
		ID:       c.nextid(),
		Name:     name,
		Schedule: schedule,
		Job:      cmd,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return *entry
		}
	}
	return Entry{}
}

// EntryName returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) EntryName(name string) Entry {
	for _, entry := range c.Entries() {
		if name == entry.Name {
			return *entry
		}
	}
	return Entry{}
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Start the cron scheduler in its own go-routine, or no-op if already started.
func (c *Cron) Start() {
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	if c.running {
		return
	}
	c.running = true
	c.run()
}

func (c *Cron) runWithRecovery(j Job) {
	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			c.logf("cron: panic running job: %v\n%s", r, buf)
		}
	}()
	j.Run()
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
func (c *Cron) Stop() {
	if !c.running {
		return
	}
	c.stop <- struct{}{}
	c.running = false
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Run the scheduler. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				now = now.In(c.location)
				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					go c.runWithRecovery(e.Job)
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)

			case <-c.snapshot:
				c.snapshot <- c.entrySnapshot()
				continue

			case id := <-c.remove:
				c.removeEntry(id)

			case <-c.stop:
				timer.Stop()
				return
			}

			break
		}
	}
}

// Logs an error to stderr or to the configured error log
func (c *Cron) logf(format string, args ...interface{}) {
	if c.ErrorLog != nil {
		c.ErrorLog.Logf(format, args...)
	}
}

//// Stop stops the cron scheduler if it is running; otherwise it does nothing.
//func (c *Cron) Stop() {
//	if !c.running {
//		return
//	}
//	c.stop <- struct{}{}
//	c.running = false
//}
func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			ID:       e.ID,
			Name:     e.Name,
			Spec:     e.Spec,
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return entries
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}
