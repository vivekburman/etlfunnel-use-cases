// seeder — generates and inserts realistic synthetic telecom data into all 4 MySQL source databases.
//
// Usage:
//   go run ./cmd/seeder [--records-per-shard N]
//
// Default: 500 records per shard (zone+state+split combination).
// For a production-scale run targeting ~3M records per company, use --records-per-shard 5000.
//
// Each company gets its own column names (matching companySchemas in mysql_schema).
// Intentional data quality issues are injected to exercise the transformer chain:
//   - ~3% null MSISDNs  (triggers NullHandler / Backlog)
//   - ~5% duplicate MSISDNs across companies (triggers DedupChecker)
//   - ~2% invalid plan codes (triggers PlanMapper backlog path)
//   - ~1% unparseable dates

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/streamcraft/telecom-etl/db_setup/internal/config"
	"github.com/streamcraft/telecom-etl/db_setup/internal/sharding"
)

var (
	recordsPerShard = flag.Int("records-per-shard", 500, "number of customer records to insert per zone+state+split shard")
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
		address: "full_address", activatedAt: "created_at",
		planCode: "product_code", planStart: "start_date", planEnd: "end_date", subStatus: "subscription_state",
		dueAmount: "balance_due", paidAmount: "balance_paid", cycleStart: "cycle_open", cycleEnd: "cycle_close",
		simSerial: "sim_id", imsi: "imsi_number", simStatus: "active_flag",
		portType: "port_event", fromCarrier: "from_network", toCarrier: "to_network",
	},
}

// Source plan codes per company (must match plan_mapping seed)
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

type shardTask struct {
	zone  string
	state string
	split int
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	log.Println("=== Telecom Synthetic Data Seeder ===")
	log.Printf("Records per shard: %d | Workers: %d\n", *recordsPerShard, *workers)

	// Build shared MSISDN pool for cross-company duplicates (5% overlap)
	sharedMSISDNs := generateSharedMSISDNPool(int(float64(*recordsPerShard) * 0.05))

	var wg sync.WaitGroup
	results := make(chan string, 100)

	for _, company := range sharding.Companies {
		wg.Add(1)
		go func(c struct {
			Name   string
			DBName string
			Port   int
		}) {
			defer wg.Done()
			if err := seedCompany(c.Name, c.DBName, c.Port, sharedMSISDNs); err != nil {
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

func seedCompany(companyName, dbName string, port int, sharedMSISDNs []string) error {
	dsn := config.MySQLDSN(config.DBHost, port, dbName)
	db, err := connectWithRetry(dsn, 15)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetMaxOpenConns(*workers * 2)
	db.SetMaxIdleConns(*workers)

	cols := colMap[companyName]
	plans := sourcePlanCodes[companyName]

	// Build shard task list
	var tasks []shardTask
	for _, zone := range sharding.Zones {
		for _, state := range zone.States {
			for split := 1; split <= sharding.DefaultSplits; split++ {
				tasks = append(tasks, shardTask{zone.Name, state, split})
			}
		}
	}

	taskCh := make(chan shardTask, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				if err := seedShard(db, companyName, cols, plans, task, sharedMSISDNs); err != nil {
					log.Printf("[%s] shard %s_%s_%d error: %v", companyName, task.zone, task.state, task.split, err)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

func seedShard(db *sql.DB, company string, cols companyColumns, plans []string, task shardTask, sharedPool []string) error {
	suffix := fmt.Sprintf("%s_%s_%d", task.zone, task.state, task.split)

	// Collect generated customer IDs for FK references in child tables
	type customerRecord struct {
		id     int64
		msisdn string
	}
	customers := make([]customerRecord, 0, *recordsPerShard)

	// Batch insert customers
	batchSize := 100
	var batch []string
	var args []interface{}
	argIdx := 1

	colNames := fmt.Sprintf(
		"(%s, %s, %s, %s, %s, %s, %s, %s, zone, state, table_split)",
		cols.msisdn, cols.name, cols.dob, cols.aadhaar, cols.pan, cols.email, cols.address, cols.activatedAt,
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query := fmt.Sprintf(
			"INSERT INTO customers_%s %s VALUES %s",
			suffix, colNames, strings.Join(batch, ","),
		)
		_, err := db.Exec(query, args...)
		batch = batch[:0]
		args = args[:0]
		argIdx = 1
		return err
	}

	for i := 0; i < *recordsPerShard; i++ {
		var msisdn interface{}
		// ~3% null MSISDNs to trigger NullHandler/Backlog
		if rand.Float64() < 0.03 {
			msisdn = nil
		} else if rand.Float64() < 0.05 && len(sharedPool) > 0 {
			// ~5% shared MSISDNs for dedup testing
			msisdn = sharedPool[rand.Intn(len(sharedPool))]
		} else {
			msisdn = generateMSISDN()
		}

		// ~1% bad date (empty string simulates unparseable)
		var dob interface{}
		if rand.Float64() < 0.01 {
			dob = "INVALID_DATE"
		} else {
			dob = randomDate(1960, 2000)
		}

		aadhaar := generateAadhaar()
		pan := generatePAN()
		name := randomName()
		email := fmt.Sprintf("%s@example.com", strings.ToLower(strings.ReplaceAll(name, " ", ".")))
		address := fmt.Sprintf("%d, Sample Street, %s, India", rand.Intn(999)+1, strings.Title(task.state))
		activatedAt := randomDate(2010, 2024)

		ph := fmt.Sprintf("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		batch = append(batch, ph)
		args = append(args,
			msisdn, name, dob, aadhaar, pan, email, address, activatedAt,
			task.zone, task.state, task.split,
		)

		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return fmt.Errorf("customer batch insert: %w", err)
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

	// Fetch inserted IDs + MSISDNs for FK use
	rows, err := db.Query(fmt.Sprintf(
		"SELECT id, %s FROM customers_%s WHERE zone=? AND state=? AND table_split=? AND %s IS NOT NULL LIMIT ?",
		cols.msisdn, suffix, cols.msisdn,
	), task.zone, task.state, task.split, *recordsPerShard)
	if err != nil {
		return fmt.Errorf("fetch customers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var m string
		if err := rows.Scan(&id, &m); err != nil {
			continue
		}
		customers = append(customers, customerRecord{id, m})
	}

	if len(customers) == 0 {
		return nil
	}

	// Seed child tables
	if err := seedSubscriptions(db, suffix, company, cols, plans, customers, task); err != nil {
		return fmt.Errorf("subscriptions: %w", err)
	}
	if err := seedBillingAccounts(db, suffix, cols, customers, task); err != nil {
		return fmt.Errorf("billing: %w", err)
	}
	if err := seedSimInventory(db, suffix, cols, customers, task); err != nil {
		return fmt.Errorf("sim_inventory: %w", err)
	}
	if err := seedPortHistory(db, suffix, cols, customers, task); err != nil {
		return fmt.Errorf("port_history: %w", err)
	}

	return nil
}

func seedSubscriptions(db *sql.DB, suffix, company string, cols companyColumns, plans []string, customers []struct {
	id     int64
	msisdn string
}, task shardTask) error {
	var rows []string
	var args []interface{}
	statuses := []string{"ACTIVE", "INACTIVE", "SUSPENDED", "EXPIRED"}

	for _, c := range customers {
		// ~2% invalid plan codes to trigger PlanMapper backlog
		var planCode string
		if rand.Float64() < 0.02 {
			planCode = "INVALID_PLAN_" + fmt.Sprintf("%04d", rand.Intn(9999))
		} else {
			planCode = plans[rand.Intn(len(plans))]
		}

		start := randomDate(2020, 2024)
		end := randomDate(2024, 2026)
		status := statuses[rand.Intn(len(statuses))]

		rows = append(rows, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, c.id, c.msisdn, planCode, start, end, status, task.zone, task.state, task.split)
	}

	return batchInsert(db,
		fmt.Sprintf("INSERT INTO subscriptions_%s (customer_id, %s, %s, %s, %s, %s, zone, state, table_split) VALUES ",
			suffix, cols.msisdn, cols.planCode, cols.planStart, cols.planEnd, cols.subStatus),
		rows, args, 200)
}

func seedBillingAccounts(db *sql.DB, suffix string, cols companyColumns, customers []struct {
	id     int64
	msisdn string
}, task shardTask) error {
	var rows []string
	var args []interface{}
	billStatuses := []string{"PAID", "UNPAID", "PARTIALLY_PAID", "OVERDUE"}

	for _, c := range customers {
		due := float64(rand.Intn(2000)) + rand.Float64()
		paid := due * rand.Float64()
		cycleStart := randomDate(2024, 2025)
		cycleEnd := randomDate(2025, 2026)
		status := billStatuses[rand.Intn(len(billStatuses))]

		rows = append(rows, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, c.id, c.msisdn,
			fmt.Sprintf("%.2f", due), fmt.Sprintf("%.2f", paid),
			cycleStart, cycleEnd, status, task.zone, task.state, task.split)
	}

	return batchInsert(db,
		fmt.Sprintf("INSERT INTO billing_accounts_%s (customer_id, %s, %s, %s, %s, %s, bill_status, zone, state, table_split) VALUES ",
			suffix, cols.msisdn, cols.dueAmount, cols.paidAmount, cols.cycleStart, cols.cycleEnd),
		rows, args, 200)
}

func seedSimInventory(db *sql.DB, suffix string, cols companyColumns, customers []struct {
	id     int64
	msisdn string
}, task shardTask) error {
	var rows []string
	var args []interface{}
	simStatuses := []string{"ACTIVE", "INACTIVE", "BLOCKED", "LOST"}

	for _, c := range customers {
		simSerial := fmt.Sprintf("89910%015d", rand.Int63n(1e15))
		imsi := fmt.Sprintf("4040%011d", rand.Int63n(1e11))
		status := simStatuses[rand.Intn(len(simStatuses))]
		activatedDate := randomDate(2015, 2024)

		rows = append(rows, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, c.id, c.msisdn, simSerial, imsi, status, activatedDate, task.zone, task.state, task.split)
	}

	return batchInsert(db,
		fmt.Sprintf("INSERT INTO sim_inventory_%s (customer_id, %s, %s, %s, %s, activated_date, zone, state, table_split) VALUES ",
			suffix, cols.msisdn, cols.simSerial, cols.imsi, cols.simStatus),
		rows, args, 200)
}

func seedPortHistory(db *sql.DB, suffix string, cols companyColumns, customers []struct {
	id     int64
	msisdn string
}, task shardTask) error {
	// Only ~20% of customers have porting history
	var rows []string
	var args []interface{}
	directions := []string{"IN", "OUT"}

	for _, c := range customers {
		if rand.Float64() > 0.20 {
			continue
		}
		dir := directions[rand.Intn(2)]
		from := carriers[rand.Intn(len(carriers))]
		to := carriers[rand.Intn(len(carriers))]
		portDate := randomDate(2018, 2024)
		ref := fmt.Sprintf("UPC%08d", rand.Intn(1e8))

		rows = append(rows, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, c.id, c.msisdn, dir, from, to, portDate, ref, task.zone, task.state, task.split)
	}

	if len(rows) == 0 {
		return nil
	}
	return batchInsert(db,
		fmt.Sprintf("INSERT INTO port_history_%s (customer_id, %s, %s, %s, %s, port_date, porting_ref, zone, state, table_split) VALUES ",
			suffix, cols.msisdn, cols.portType, cols.fromCarrier, cols.toCarrier),
		rows, args, 200)
}

// ---------- Helpers ----------

func batchInsert(db *sql.DB, prefix string, rows []string, args []interface{}, batchSize int) error {
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunkRows := rows[start:end]
		chunkArgs := args[start*len(args)/len(rows) : end*len(args)/len(rows)]
		// Approximate arg split — use exact calculation
		argsPerRow := len(args) / len(rows)
		chunkArgs = args[start*argsPerRow : end*argsPerRow]

		query := prefix + strings.Join(chunkRows, ",")
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
	// Indian mobile numbers: 10 digits starting with 6-9
	prefixes := []string{"6", "7", "8", "9"}
	prefix := prefixes[rand.Intn(len(prefixes))]
	return "9" + prefix + fmt.Sprintf("%08d", rand.Intn(1e8))
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
