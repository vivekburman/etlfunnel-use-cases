// seeder — generates and inserts realistic synthetic telecom data into all 4 MySQL source databases.
//
// Usage:
//   go run ./cmd/seeder [--records-per-shard N]
//
// Default: 500 records per shard.
// Dynamic table splits: when a shard table reaches sharding.SplitRowCap (1,000,000 rows),
// the seeder creates the next split table on the fly (_2, _3, …) and continues inserting there.
//
// Sharding strategy per company:
//   vodafone    — zone + state  (customers_north_up_1, _2, …)
//   idea        — zone only     (customers_north_1, _2, …)
//   tata_docomo — state only    (customers_up_1, _2, …)
//   aircel      — zone + state  (customers_north_up_1, _2, …)
//
// Intentional data quality issues injected to exercise the transformer chain:
//   ~3% null MSISDNs          (triggers NullHandler / Backlog)
//   ~5% duplicate MSISDNs     (triggers DedupChecker)
//   ~2% invalid plan codes    (triggers PlanMapper / Backlog)
//   ~1% unparseable dates     (activation_date set to "0000-00-00 00:00:00" via sql_mode override
//                              — triggers TypeCaster ParseDate failure / Backlog)

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/streamcraft/telecom-etl/db_setup/internal/config"
	"github.com/streamcraft/telecom-etl/db_setup/internal/sharding"
)

const insertBatchSize = 5000

var (
	recordsPerShard = flag.Int("records-per-shard", 500, "number of customer records to insert per shard")
	workers         = flag.Int("workers", 4, "parallel worker goroutines per company")
)

// ---------- Column name map (mirrors mysql_schema companySchemas) ----------

type companyColumns struct {
	msisdn, name, dob, aadhaar, pan, email, address, activatedAt string
	planCode, planStart, planEnd, subStatus                       string
	dueAmount, paidAmount, cycleStart, cycleEnd                   string
	simSerial, imsi, simStatus                                    string
	portType, fromCarrier, toCarrier                              string
}

var colMap = map[string]companyColumns{
	"vodafone": {
		msisdn: "mob_no", name: "cust_name", dob: "date_of_birth",
		aadhaar: "aadhaar_num", pan: "pan_num", email: "email_id",
		address: "address", activatedAt: "activation_date",
		planCode: "plan_code", planStart: "plan_start", planEnd: "plan_end", subStatus: "sub_status",
		dueAmount: "amount_due", paidAmount: "amount_paid", cycleStart: "cycle_from", cycleEnd: "cycle_to",
		simSerial: "sim_serial", imsi: "imsi_no", simStatus: "sim_status",
		portType: "port_direction", fromCarrier: "from_operator", toCarrier: "to_operator",
	},
	"idea": {
		msisdn: "contact", name: "full_name", dob: "dob",
		aadhaar: "aadhaar", pan: "pan", email: "email",
		address: "addr", activatedAt: "activated_on",
		planCode: "tariff_code", planStart: "validity_start", planEnd: "validity_end", subStatus: "status",
		dueAmount: "outstanding", paidAmount: "paid", cycleStart: "bill_from", cycleEnd: "bill_to",
		simSerial: "serial_no", imsi: "imsi", simStatus: "status",
		portType: "mnp_type", fromCarrier: "source_operator", toCarrier: "dest_operator",
	},
	"tata_docomo": {
		msisdn: "phone_number", name: "subscriber_name", dob: "birth_date",
		aadhaar: "id_aadhaar", pan: "id_pan", email: "subscriber_email",
		address: "residential_address", activatedAt: "sim_activation_dt",
		planCode: "pack_code", planStart: "pack_start_dt", planEnd: "pack_end_dt", subStatus: "pack_status",
		dueAmount: "dues", paidAmount: "payments_received", cycleStart: "period_start", cycleEnd: "period_end",
		simSerial: "iccid", imsi: "imsi_code", simStatus: "sim_active",
		portType: "port_in_out", fromCarrier: "prev_carrier", toCarrier: "new_carrier",
	},
	"aircel": {
		msisdn: "msisdn", name: "name", dob: "dob",
		aadhaar: "aadhaar_id", pan: "pan_id", email: "email_address",
		address: "full_address", activatedAt: "activated_at",
		planCode: "product_code", planStart: "start_date", planEnd: "end_date", subStatus: "subscription_state",
		dueAmount: "balance_due", paidAmount: "balance_paid", cycleStart: "cycle_open", cycleEnd: "cycle_close",
		simSerial: "sim_id", imsi: "imsi_number", simStatus: "active_flag",
		portType: "port_event", fromCarrier: "from_network", toCarrier: "to_network",
	},
}

var sourcePlanCodes = map[string][]string{
	"vodafone":    {"VF_49", "VF_149", "VF_299", "VF_599", "VF_999", "VF_PREPAID_10"},
	"idea":        {"ID_49", "ID_199", "ID_349", "ID_449", "ID_595", "ID_1199"},
	"tata_docomo": {"TD_52", "TD_155", "TD_255", "TD_455", "TD_755", "TD_ANNUAL"},
	"aircel":      {"AC_44", "AC_98", "AC_198", "AC_398", "AC_598", "AC_1098"},
}

var carriers = []string{"Vodafone", "Idea", "Tata Docomo", "Aircel", "BSNL", "MTNL", "Reliance"}

var firstNames = []string{
	"Rahul", "Priya", "Amit", "Sneha", "Vijay", "Kavya", "Rohan", "Pooja",
	"Suresh", "Anita", "Deepak", "Meena", "Arun", "Sunita", "Manoj", "Nisha",
	"Rajesh", "Geeta", "Sanjay", "Rekha", "Sunil", "Usha", "Naveen", "Lata",
	"Ramesh", "Sarla", "Dinesh", "Preeti", "Ashok", "Seema",
}
var lastNames = []string{
	"Sharma", "Verma", "Gupta", "Singh", "Kumar", "Patel", "Joshi", "Nair",
	"Reddy", "Rao", "Mehta", "Shah", "Malhotra", "Kapoor", "Iyer", "Pillai",
	"Chatterjee", "Mukherjee", "Bose", "Das", "Mishra", "Pandey", "Tiwari", "Dubey",
}

// ---------- Shard task ----------

// shardTask identifies a single shard to seed.
// For zone-only companies, state is empty. For state-only, zone is empty.
type shardTask struct {
	zone  string
	state string
}

// key returns a unique string for this shard (used for logging).
func (t shardTask) key() string {
	if t.zone != "" && t.state != "" {
		return t.zone + "_" + t.state
	}
	if t.zone != "" {
		return t.zone
	}
	return t.state
}

// suffix builds the table name suffix for a given split number.
func (t shardTask) suffix(split int) string {
	return fmt.Sprintf("%s_%d", t.key(), split)
}

// hasZone / hasState report which geo dimensions are present for this company's sharding type.
// They are derived from which fields are populated, so we don't need to thread ShardingType
// all the way down into every helper.
func (t shardTask) hasZone() bool  { return t.zone != "" }
func (t shardTask) hasState() bool { return t.state != "" }

// ---------- customerRecord ----------

type customerRecord struct {
	id     int64
	msisdn string
	split  int
}

// ---------- main ----------

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	mysql.SetLogger(log.New(io.Discard, "", 0))
	log.Println("=== Telecom Synthetic Data Seeder ===")
	log.Printf("Records per shard: %d | Workers: %d | Split cap: %d\n",
		*recordsPerShard, *workers, sharding.SplitRowCap)

	sharedMSISDNs := generateSharedMSISDNPool(int(float64(*recordsPerShard) * 0.05))

	var wg sync.WaitGroup
	results := make(chan string, 100)

	for _, company := range sharding.Companies {
		wg.Add(1)
		go func(c struct {
			Name         string
			DBName       string
			Port         int
			ShardingType sharding.ShardingType
		}) {
			defer wg.Done()
			if err := seedCompany(c.Name, c.DBName, c.Port, c.ShardingType, sharedMSISDNs); err != nil {
				results <- fmt.Sprintf("[%s] FAILED: %v", c.Name, err)
			} else {
				results <- fmt.Sprintf("[%s] ✓ seeding complete", c.Name)
			}
		}(company)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for msg := range results {
		log.Println(msg)
	}
	log.Println("=== Seeding complete ===")
}

func generateSharedMSISDNPool(n int) []string {
	pool := make([]string, n)
	for i := 0; i < n; i++ {
		pool[i] = generateMSISDN()
	}
	return pool
}

// ---------- Company-level seeding ----------

func seedCompany(companyName, dbName string, port int, st sharding.ShardingType, sharedMSISDNs []string) error {
	dsn := config.MySQLDSN(config.DBHost, port, dbName)
	db, err := connectWithRetry(dsn, 15)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetMaxOpenConns(*workers * 2)
	db.SetMaxIdleConns(*workers)

	tasks := buildTasks(st)

	taskCh := make(chan shardTask, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	var wg sync.WaitGroup
	cols := colMap[companyName]
	plans := sourcePlanCodes[companyName]

	var mu sync.Mutex
	var shardsOK, shardsFailed int

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				if err := seedShard(db, companyName, cols, plans, task, sharedMSISDNs); err != nil {
					mu.Lock()
					shardsFailed++
					mu.Unlock()
					log.Printf("[%s] shard %-25s SKIPPED — %v", companyName, task.key(), err)
				} else {
					mu.Lock()
					shardsOK++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	log.Printf("[%s] shards seeded: %d ok, %d skipped (bad-data batches)", companyName, shardsOK, shardsFailed)
	return nil
}

// buildTasks returns the shard task list for the given sharding strategy.
func buildTasks(st sharding.ShardingType) []shardTask {
	var tasks []shardTask
	switch st {
	case sharding.ZoneState:
		for _, z := range sharding.Zones {
			for _, state := range z.States {
				tasks = append(tasks, shardTask{zone: z.Name, state: state})
			}
		}
	case sharding.ZoneOnly:
		for _, z := range sharding.Zones {
			tasks = append(tasks, shardTask{zone: z.Name})
		}
	case sharding.StateOnly:
		for _, state := range sharding.AllStates() {
			tasks = append(tasks, shardTask{state: state})
		}
	}
	return tasks
}

// ---------- Shard-level seeding ----------

// shardState describes the current persistence state of a shard across all its splits.
type shardState struct {
	totalRows       int // rows already in the DB across all splits
	currentSplit    int // highest split that exists (1-based)
	currentSplitRows int // row count in that split (determines remaining capacity)
}

// inspectExistingSplits walks split tables _1, _2, _3 … until one is missing,
// returning the aggregate row count and the state of the last existing split.
// A brand-new shard (split _1 just created by mysql_schema but empty) returns
// shardState{0, 1, 0} so seeding starts cleanly into _1.
func inspectExistingSplits(db *sql.DB, task shardTask) (shardState, error) {
	var st shardState
	st.currentSplit = 1

	for n := 1; ; n++ {
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM customers_%s", task.suffix(n))).Scan(&count)
		if err != nil {
			// Table does not exist — n-1 was the last split.
			st.currentSplit = n - 1
			if st.currentSplit < 1 {
				st.currentSplit = 1
				st.currentSplitRows = 0
			}
			return st, nil
		}
		st.totalRows += count
		st.currentSplitRows = count
		st.currentSplit = n
	}
}

func seedShard(db *sql.DB, company string, cols companyColumns, plans []string, task shardTask, sharedPool []string) error {
	existing, err := inspectExistingSplits(db, task)
	if err != nil {
		return fmt.Errorf("inspect splits: %w", err)
	}

	toInsert := *recordsPerShard - existing.totalRows
	if toInsert <= 0 {
		log.Printf("  shard %s: already at %d rows (target %d), skipping",
			task.key(), existing.totalRows, *recordsPerShard)
		return nil
	}
	if existing.totalRows > 0 {
		log.Printf("  shard %s: resuming — %d existing rows, inserting %d more",
			task.key(), existing.totalRows, toInsert)
	}

	splitNum := existing.currentSplit
	splitRowCount := existing.currentSplitRows

	var allCustomers []customerRecord

	batchSize := insertBatchSize
	type rowData struct {
		msisdn      interface{}
		name        string
		dob         interface{}
		aadhaar     string
		pan         string
		email       string
		address     string
		activatedAt string
	}

	var pending []rowData

	flushCustomers := func() error {
		if len(pending) == 0 {
			return nil
		}

		geoNames, geoPlaceholders := geoInsertParts(task)
		colNames := fmt.Sprintf("(%s, %s, %s, %s, %s, %s, %s, %s%s, table_split)",
			cols.msisdn, cols.name, cols.dob, cols.aadhaar, cols.pan, cols.email, cols.address, cols.activatedAt,
			geoNames,
		)

		var placeholders []string
		var args []interface{}
		argsPerRow := 9 + len(geoPlaceholders) // 8 data cols + table_split + geo cols
		_ = argsPerRow
		for _, r := range pending {
			ph := fmt.Sprintf("(?, ?, ?, ?, ?, ?, ?, ?%s, ?)", strings.Repeat(", ?", len(geoPlaceholders)))
			placeholders = append(placeholders, ph)
			args = append(args, r.msisdn, r.name, r.dob, r.aadhaar, r.pan, r.email, r.address, r.activatedAt)
			args = append(args, geoPlaceholders...)
			args = append(args, splitNum)
		}

		query := fmt.Sprintf("INSERT INTO customers_%s %s VALUES %s",
			task.suffix(splitNum), colNames, strings.Join(placeholders, ","))

		result, err := db.Exec(query, args...)
		if err != nil {
			return err
		}

		firstID, err := result.LastInsertId()
		if err != nil {
			return err
		}
		for i, r := range pending {
			var m string
			if r.msisdn != nil {
				m = r.msisdn.(string)
			}
			if m != "" {
				allCustomers = append(allCustomers, customerRecord{
					id:     firstID + int64(i),
					msisdn: m,
					split:  splitNum,
				})
			}
		}

		splitRowCount += len(pending)
		pending = pending[:0]
		return nil
	}

	for i := 0; i < toInsert; i++ {
		// Check if we've hit the split cap — flush first, then advance split.
		if splitRowCount >= sharding.SplitRowCap {
			if err := flushCustomers(); err != nil {
				return fmt.Errorf("flush before split: %w", err)
			}
			splitNum++
			splitRowCount = 0
			if err := createSplitTables(db, task.suffix(splitNum), company, cols); err != nil {
				return fmt.Errorf("create split %d tables: %w", splitNum, err)
			}
			log.Printf("  shard %s: created split _%d", task.key(), splitNum)
		}

		var msisdn interface{}
		if rand.Float64() < 0.03 {
			msisdn = nil
		} else if rand.Float64() < 0.05 && len(sharedPool) > 0 {
			msisdn = sharedPool[rand.Intn(len(sharedPool))]
		} else {
			msisdn = generateMSISDN()
		}

		var dob interface{}
		if rand.Float64() < 0.01 {
			dob = nil // intentional null DOB (~1%) — exercises NullHandler/Backlog in the ETL pipeline
		} else {
			dob = randomDate(1960, 2000)
		}

		name := randomName()
		pending = append(pending, rowData{
			msisdn:      msisdn,
			name:        name,
			dob:         dob,
			aadhaar:     generateAadhaar(),
			pan:         generatePAN(),
			email:       fmt.Sprintf("%s@example.com", strings.ToLower(strings.ReplaceAll(name, " ", "."))),
			address:     fmt.Sprintf("%d, Sample Street, %s, India", rand.Intn(999)+1, strings.Title(task.state+task.zone)),
			activatedAt: randomDate(2010, 2024),
		})

		if len(pending) >= batchSize {
			if err := flushCustomers(); err != nil {
				return fmt.Errorf("customer batch: %w", err)
			}
		}
	}
	if err := flushCustomers(); err != nil {
		return fmt.Errorf("customer batch: %w", err)
	}

	if len(allCustomers) == 0 {
		return nil
	}

	// Inject ~1% zero-dates into activatedAt to exercise TypeCaster ParseDate → Backlog.
	if err := injectBadActivationDates(db, task, cols.activatedAt, allCustomers); err != nil {
		log.Printf("  shard %s: bad-date injection warning — %v", task.key(), err)
	}

	// Group customers by split so child tables land in the matching split table.
	bySplit := map[int][]customerRecord{}
	for _, c := range allCustomers {
		bySplit[c.split] = append(bySplit[c.split], c)
	}

	for split, splitCustomers := range bySplit {
		suffix := task.suffix(split)
		if err := seedSubscriptions(db, suffix, cols, plans, splitCustomers, task); err != nil {
			return fmt.Errorf("subscriptions split %d: %w", split, err)
		}
		if err := seedBillingAccounts(db, suffix, cols, splitCustomers, task); err != nil {
			return fmt.Errorf("billing split %d: %w", split, err)
		}
		if err := seedSimInventory(db, suffix, cols, splitCustomers, task); err != nil {
			return fmt.Errorf("sim_inventory split %d: %w", split, err)
		}
		if err := seedPortHistory(db, suffix, cols, splitCustomers, task); err != nil {
			return fmt.Errorf("port_history split %d: %w", split, err)
		}
	}

	return nil
}

// geoInsertParts returns the column name fragment and value slice for zone/state geo columns.
// Column names come back as ", zone, state" (or subset); values come back as interface{} slice.
func geoInsertParts(task shardTask) (colFragment string, values []interface{}) {
	if task.hasZone() {
		colFragment += ", zone"
		values = append(values, task.zone)
	}
	if task.hasState() {
		colFragment += ", state"
		values = append(values, task.state)
	}
	return colFragment, values
}

// ---------- Child table seeders ----------

func seedSubscriptions(db *sql.DB, suffix string, cols companyColumns, plans []string, customers []customerRecord, task shardTask) error {
	statuses := []string{"ACTIVE", "INACTIVE", "SUSPENDED", "EXPIRED"}
	geoNames, geoVals := geoInsertParts(task)

	var rows []string
	var args []interface{}
	for _, c := range customers {
		var planCode string
		if rand.Float64() < 0.02 {
			planCode = fmt.Sprintf("INVALID_PLAN_%04d", rand.Intn(9999))
		} else {
			planCode = plans[rand.Intn(len(plans))]
		}

		ph := fmt.Sprintf("(?, ?, ?, ?, ?, ?%s, ?)", strings.Repeat(", ?", len(geoVals)))
		rows = append(rows, ph)
		args = append(args, c.id, c.msisdn, planCode, randomDate(2020, 2024), randomDate(2024, 2026),
			statuses[rand.Intn(len(statuses))])
		args = append(args, geoVals...)
		args = append(args, c.split)
	}

	prefix := fmt.Sprintf(
		"INSERT INTO subscriptions_%s (customer_id, %s, %s, %s, %s, %s%s, table_split) VALUES ",
		suffix, cols.msisdn, cols.planCode, cols.planStart, cols.planEnd, cols.subStatus, geoNames,
	)
	return batchInsert(db, prefix, rows, args, insertBatchSize)
}

func seedBillingAccounts(db *sql.DB, suffix string, cols companyColumns, customers []customerRecord, task shardTask) error {
	billStatuses := []string{"PAID", "UNPAID", "PARTIALLY_PAID", "OVERDUE"}
	geoNames, geoVals := geoInsertParts(task)

	var rows []string
	var args []interface{}
	for _, c := range customers {
		due := float64(rand.Intn(2000)) + rand.Float64()
		paid := due * rand.Float64()

		ph := fmt.Sprintf("(?, ?, ?, ?, ?, ?, ?%s, ?)", strings.Repeat(", ?", len(geoVals)))
		rows = append(rows, ph)
		args = append(args, c.id, c.msisdn,
			fmt.Sprintf("%.2f", due), fmt.Sprintf("%.2f", paid),
			randomDate(2024, 2025), randomDate(2025, 2026),
			billStatuses[rand.Intn(len(billStatuses))])
		args = append(args, geoVals...)
		args = append(args, c.split)
	}

	prefix := fmt.Sprintf(
		"INSERT INTO billing_accounts_%s (customer_id, %s, %s, %s, %s, %s, bill_status%s, table_split) VALUES ",
		suffix, cols.msisdn, cols.dueAmount, cols.paidAmount, cols.cycleStart, cols.cycleEnd, geoNames,
	)
	return batchInsert(db, prefix, rows, args, insertBatchSize)
}

func seedSimInventory(db *sql.DB, suffix string, cols companyColumns, customers []customerRecord, task shardTask) error {
	simStatuses := []string{"ACTIVE", "INACTIVE", "BLOCKED", "LOST"}
	geoNames, geoVals := geoInsertParts(task)

	var rows []string
	var args []interface{}
	for _, c := range customers {
		ph := fmt.Sprintf("(?, ?, ?, ?, ?, ?%s, ?)", strings.Repeat(", ?", len(geoVals)))
		rows = append(rows, ph)
		args = append(args, c.id, c.msisdn,
			fmt.Sprintf("89910%015d", rand.Int63n(1e15)),
			fmt.Sprintf("4040%011d", rand.Int63n(1e11)),
			simStatuses[rand.Intn(len(simStatuses))],
			randomDate(2015, 2024))
		args = append(args, geoVals...)
		args = append(args, c.split)
	}

	prefix := fmt.Sprintf(
		"INSERT INTO sim_inventory_%s (customer_id, %s, %s, %s, %s, activated_date%s, table_split) VALUES ",
		suffix, cols.msisdn, cols.simSerial, cols.imsi, cols.simStatus, geoNames,
	)
	return batchInsert(db, prefix, rows, args, insertBatchSize)
}

func seedPortHistory(db *sql.DB, suffix string, cols companyColumns, customers []customerRecord, task shardTask) error {
	directions := []string{"IN", "OUT"}
	geoNames, geoVals := geoInsertParts(task)

	var rows []string
	var args []interface{}
	for _, c := range customers {
		if rand.Float64() > 0.20 {
			continue
		}
		ph := fmt.Sprintf("(?, ?, ?, ?, ?, ?, ?%s, ?)", strings.Repeat(", ?", len(geoVals)))
		rows = append(rows, ph)
		args = append(args, c.id, c.msisdn,
			directions[rand.Intn(2)],
			carriers[rand.Intn(len(carriers))],
			carriers[rand.Intn(len(carriers))],
			randomDate(2018, 2024),
			fmt.Sprintf("UPC%08d", rand.Intn(1e8)))
		args = append(args, geoVals...)
		args = append(args, c.split)
	}

	if len(rows) == 0 {
		return nil
	}
	prefix := fmt.Sprintf(
		"INSERT INTO port_history_%s (customer_id, %s, %s, %s, %s, port_date, porting_ref%s, table_split) VALUES ",
		suffix, cols.msisdn, cols.portType, cols.fromCarrier, cols.toCarrier, geoNames,
	)
	return batchInsert(db, prefix, rows, args, insertBatchSize)
}

// ---------- Dynamic split table creation ----------

// createSplitTables creates all 5 table types for a new split suffix.
// Called by the seeder when SplitRowCap is reached; mirrors the DDL in mysql_schema.
func createSplitTables(db *sql.DB, suffix, company string, cols companyColumns) error {
	s := toDDLSchema(cols)
	hasZone := true  // determined by which cols are non-empty; approximated by company
	hasState := true // both are safe defaults — columns are present when geo args are supplied

	// Derive from column presence: zone-only companies (idea) have no state-specific cols,
	// but since companyColumns doesn't carry geo metadata, we use the suffix as a signal.
	// Suffixes for zone-only look like "north_2" (one word before the split digit).
	// Suffixes for state-only look like "up_2". Zone+state look like "north_up_2".
	parts := strings.Split(suffix, "_")
	// last part is the split number; remaining parts are the geo key
	geoKey := strings.Join(parts[:len(parts)-1], "_")
	zoneParts := 0
	for _, z := range sharding.Zones {
		if z.Name == geoKey {
			zoneParts = 1 // zone-only
			break
		}
		for _, st := range z.States {
			if st == geoKey {
				zoneParts = -1 // state-only
				break
			}
		}
		if zoneParts != 0 {
			break
		}
	}
	switch zoneParts {
	case 1:
		hasZone, hasState = true, false
	case -1:
		hasZone, hasState = false, true
	default:
		hasZone, hasState = true, true // zone_state
	}

	ddls := []string{
		customersDDL(suffix, s, hasZone, hasState),
		subscriptionsDDL(suffix, s, hasZone, hasState),
		billingAccountsDDL(suffix, s, hasZone, hasState),
		simInventoryDDL(suffix, s, hasZone, hasState),
		portHistoryDDL(suffix, s, hasZone, hasState),
	}
	for _, ddl := range ddls {
		if _, err := db.Exec(ddl); err != nil {
			return err
		}
	}
	return nil
}

// ddlSchema mirrors mysql_schema's companySchema but sourced from companyColumns.
type ddlSchema struct {
	msisdn, name, dob, aadhaar, pan, email, address, activatedAt string
	planCode, planStart, planEnd, status                          string
	dueAmount, paidAmount, cycleStart, cycleEnd                   string
	simSerial, imsi, simStatus                                    string
	portType, fromCarrier, toCarrier                              string
}

func toDDLSchema(c companyColumns) ddlSchema {
	return ddlSchema{
		msisdn: c.msisdn, name: c.name, dob: c.dob, aadhaar: c.aadhaar, pan: c.pan,
		email: c.email, address: c.address, activatedAt: c.activatedAt,
		planCode: c.planCode, planStart: c.planStart, planEnd: c.planEnd, status: c.subStatus,
		dueAmount: c.dueAmount, paidAmount: c.paidAmount, cycleStart: c.cycleStart, cycleEnd: c.cycleEnd,
		simSerial: c.simSerial, imsi: c.imsi, simStatus: c.simStatus,
		portType: c.portType, fromCarrier: c.fromCarrier, toCarrier: c.toCarrier,
	}
}

func geoColsDDL(hasZone, hasState bool) string {
	switch {
	case hasZone && hasState:
		return "zone            VARCHAR(20)  NOT NULL,\n    state           VARCHAR(50)  NOT NULL,"
	case hasZone:
		return "zone            VARCHAR(20)  NOT NULL,"
	case hasState:
		return "state           VARCHAR(50)  NOT NULL,"
	default:
		return ""
	}
}

func geoIdxDDL(hasZone, hasState bool) string {
	switch {
	case hasZone && hasState:
		return "INDEX idx_zone_state (zone, state)"
	case hasZone:
		return "INDEX idx_zone (zone)"
	case hasState:
		return "INDEX idx_state (state)"
	default:
		return ""
	}
}

func customersDDL(suffix string, s ddlSchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS customers_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    %s              VARCHAR(15)  COMMENT 'msisdn',
    %s              VARCHAR(100) NOT NULL COMMENT 'name',
    %s              DATE         COMMENT 'dob',
    %s              VARCHAR(20)  COMMENT 'aadhaar',
    %s              VARCHAR(15)  COMMENT 'pan',
    %s              VARCHAR(150) COMMENT 'email',
    %s              TEXT         COMMENT 'address',
    %s              DATETIME     COMMENT 'activation_date',
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    %s
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdn, s.name, s.dob, s.aadhaar, s.pan, s.email, s.address, s.activatedAt,
		geoColsDDL(hasZone, hasState),
		s.msisdn,
		geoIdxDDL(hasZone, hasState),
	)
}

func subscriptionsDDL(suffix string, s ddlSchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS subscriptions_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id     BIGINT UNSIGNED NOT NULL,
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              VARCHAR(50)  NOT NULL COMMENT 'plan_code',
    %s              DATE         COMMENT 'plan_start',
    %s              DATE         COMMENT 'plan_end',
    %s              VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_plan (%s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdn, s.planCode, s.planStart, s.planEnd, s.status,
		geoColsDDL(hasZone, hasState),
		s.msisdn, s.planCode,
	)
}

func billingAccountsDDL(suffix string, s ddlSchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS billing_accounts_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id     BIGINT UNSIGNED NOT NULL,
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    %s              DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    %s              DATE         COMMENT 'cycle_start',
    %s              DATE         COMMENT 'cycle_end',
    bill_status     VARCHAR(20)  NOT NULL DEFAULT 'UNPAID',
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_cycle (%s, %s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdn, s.dueAmount, s.paidAmount, s.cycleStart, s.cycleEnd,
		geoColsDDL(hasZone, hasState),
		s.msisdn, s.cycleStart, s.cycleEnd,
	)
}

func simInventoryDDL(suffix string, s ddlSchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS sim_inventory_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id     BIGINT UNSIGNED NOT NULL,
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              VARCHAR(22)  NOT NULL COMMENT 'sim_serial',
    %s              VARCHAR(15)  NOT NULL COMMENT 'imsi',
    %s              VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    activated_date  DATE         COMMENT 'sim activation date',
    deactivated_date DATE,
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    UNIQUE idx_iccid (%s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdn, s.simSerial, s.imsi, s.simStatus,
		geoColsDDL(hasZone, hasState),
		s.msisdn, s.simSerial,
	)
}

func portHistoryDDL(suffix string, s ddlSchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS port_history_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id     BIGINT UNSIGNED NOT NULL,
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              VARCHAR(10)  NOT NULL COMMENT 'port direction',
    %s              VARCHAR(50)  COMMENT 'from_carrier',
    %s              VARCHAR(50)  COMMENT 'to_carrier',
    port_date       DATE         NOT NULL,
    porting_ref     VARCHAR(30),
    status          VARCHAR(20)  NOT NULL DEFAULT 'COMPLETED',
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_port_date (port_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdn, s.portType, s.fromCarrier, s.toCarrier,
		geoColsDDL(hasZone, hasState),
		s.msisdn,
	)
}

// ---------- Bad-data injectors ----------

// injectBadActivationDates corrupts ~1% of customer activation_date values to "0000-00-00 00:00:00".
// MySQL's NO_ZERO_DATE strict mode rejects zero dates on normal inserts, so we temporarily
// disable the session sql_mode, run the UPDATE, then restore it — all on a dedicated connection
// so the mode change doesn't leak to other pool connections.
func injectBadActivationDates(db *sql.DB, task shardTask, activatedAtCol string, customers []customerRecord) error {
	bySplit := map[int][]int64{}
	for _, c := range customers {
		if rand.Float64() < 0.01 {
			bySplit[c.split] = append(bySplit[c.split], c.id)
		}
	}
	if len(bySplit) == 0 {
		return nil
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(context.Background(), "SET SESSION sql_mode = ''"); err != nil {
		return err
	}
	// Always restore the MySQL 8.0 default strict mode before returning the connection to the pool.
	defer conn.ExecContext(context.Background(), //nolint
		"SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
	)

	for split, ids := range bySplit {
		placeholders := make([]string, len(ids))
		args := make([]interface{}, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args[i] = id
		}
		query := fmt.Sprintf(
			"UPDATE customers_%s SET %s = '0000-00-00 00:00:00' WHERE id IN (%s)",
			task.suffix(split), activatedAtCol, strings.Join(placeholders, ","),
		)
		if _, err := conn.ExecContext(context.Background(), query, args...); err != nil {
			return err
		}
		log.Printf("  shard %s split %d: corrupted %d activation_date(s) to zero-date for backlog testing",
			task.key(), split, len(ids))
	}
	return nil
}

// ---------- Helpers ----------

func batchInsert(db *sql.DB, prefix string, rows []string, args []interface{}, batchSize int) error {
	if len(rows) == 0 {
		return nil
	}
	argsPerRow := len(args) / len(rows)
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunkArgs := args[start*argsPerRow : end*argsPerRow]
		query := prefix + strings.Join(rows[start:end], ",")
		if _, err := db.Exec(query, chunkArgs...); err != nil {
			return err
		}
	}
	return nil
}

func connectWithRetry(dsn string, retries int) (*sql.DB, error) {
	for i := 0; i < retries; i++ {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				return db, nil
			}
		}
		log.Printf("  waiting for MySQL... (%d/%d)", i+1, retries)
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("could not connect after %d retries", retries)
}

func generateMSISDN() string {
	prefixes := []string{"6", "7", "8", "9"}
	return "9" + prefixes[rand.Intn(len(prefixes))] + fmt.Sprintf("%08d", rand.Intn(1e8))
}

func generateAadhaar() string {
	return fmt.Sprintf("%04d %04d %04d", rand.Intn(9000)+1000, rand.Intn(9000)+1000, rand.Intn(9000)+1000)
}

func generatePAN() string {
	letters := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	pan := ""
	for i := 0; i < 5; i++ {
		pan += string(letters[rand.Intn(len(letters))])
	}
	pan += fmt.Sprintf("%04d", rand.Intn(9000)+1000)
	pan += string(letters[rand.Intn(len(letters))])
	return pan
}

func randomName() string {
	return firstNames[rand.Intn(len(firstNames))] + " " + lastNames[rand.Intn(len(lastNames))]
}

func randomDate(fromYear, toYear int) string {
	year := fromYear + rand.Intn(toYear-fromYear)
	month := rand.Intn(12) + 1
	day := rand.Intn(28) + 1
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}
