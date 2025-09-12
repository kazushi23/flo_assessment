package mysql

import (
	"flo/assessment/config/toml"
	"flo/assessment/entity"
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var _db *gorm.DB
var _bulkOptimized bool

func init() {
	username := toml.GetConfig().Mysql.User
	password := toml.GetConfig().Mysql.Password
	host := toml.GetConfig().Mysql.Host
	port := toml.GetConfig().Mysql.Port
	dbname := toml.GetConfig().Mysql.DbName

	// OPTIMIZED DSN with bulk insert parameters
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=UTC&timeout=30s&readTimeout=30s&writeTimeout=60s&interpolateParams=true&multiStatements=true&maxAllowedPacket=0",
		username, password, host, port, dbname)

	fmt.Println("DSN:", dsn)
	var err error

	// Use Silent logger for bulk operations
	var newLogger logger.Interface
	if toml.GetConfig().Environment == "production" { // or add your env check
		newLogger = logger.Default.LogMode(logger.Silent) // NO SQL logging in production
	} else {
		newLogger = logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			logger.Config{
				SlowThreshold:             2 * time.Second, // Increased threshold
				LogLevel:                  logger.Warn,     // Only log warnings/errors
				IgnoreRecordNotFoundError: true,
				Colorful:                  true,
			},
		)
	}

	_db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:                                   newLogger,
		SkipDefaultTransaction:                   true, // Disable auto-transactions
		PrepareStmt:                              true, // Cache prepared statements
		DisableForeignKeyConstraintWhenMigrating: true, // Faster migrations
	})

	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	sqlDB, err := _db.DB()
	if err != nil {
		log.Fatal("Failed to get underlying sql.DB:", err)
	}

	// Auto-migrate first
	if err := _db.AutoMigrate(
		&entity.MeterReadingsEntity{},
		&entity.StagingMeterReadingEntity{},
		&entity.ErrorRowsEntity{},
	); err != nil {
		log.Fatal("Migration failed:", err)
	}

	// OPTIMIZED connection pool settings
	maxOpen := toml.GetConfig().Process.Maxdbconnections
	maxIdle := toml.GetConfig().Process.Maxdbidleconnections

	fmt.Printf("Setting DB pool: MaxOpen=%d, MaxIdle=%d\n", maxOpen, maxIdle)

	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	sqlDB.SetConnMaxLifetime(1 * time.Hour)    // Longer lifetime
	sqlDB.SetConnMaxIdleTime(10 * time.Minute) // Keep connections alive longer

	// Test connection
	if err := sqlDB.Ping(); err != nil {
		log.Fatal("Database ping failed:", err)
	}

	fmt.Println("Database connection established successfully")
}

func GetDB() *gorm.DB {
	return _db
}

// Call this before bulk operations
func EnableBulkOptimizations() error {
	if _bulkOptimized {
		return nil
	}

	sqlCommands := []string{
		"SET SESSION sql_log_bin = 0",
		"SET SESSION foreign_key_checks = 0",
		"SET SESSION sort_buffer_size = 32*1024*1024",
		"SET SESSION autocommit = 1",
		"SET SESSION unique_checks = 0",
	}

	for _, cmd := range sqlCommands {
		if err := _db.Exec(cmd).Error; err != nil {
			fmt.Printf("Warning: Failed to execute optimization: %s - %v\n", cmd, err)
		} else {
			fmt.Printf("Applied: %s\n", cmd)
		}
	}

	_bulkOptimized = true
	fmt.Println("Bulk insert optimizations enabled")
	return nil
}

// Call this after bulk operations
func DisableBulkOptimizations() error {
	if !_bulkOptimized {
		return nil
	}

	restoreCommands := []string{
		"SET SESSION sql_log_bin = 1",
		"SET SESSION foreign_key_checks = 1",
		"SET SESSION unique_checks = 1",
	}

	for _, cmd := range restoreCommands {
		if err := _db.Exec(cmd).Error; err != nil {
			fmt.Printf("Warning: Failed to restore setting: %s - %v\n", cmd, err)
		}
	}

	_bulkOptimized = false
	fmt.Println("Database settings restored")
	return nil
}

func Save(value interface{}) {
	result := _db.Save(value)
	if result.RowsAffected == 0 {
		fmt.Println("No Rows affected")
	}
}
