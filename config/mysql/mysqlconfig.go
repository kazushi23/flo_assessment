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

func init() {
	username := toml.GetConfig().Mysql.User     // this is for db username connection
	password := toml.GetConfig().Mysql.Password // this is for db password connection
	host := toml.GetConfig().Mysql.Host         // this is for db host connection
	port := toml.GetConfig().Mysql.Port         // this is for db port connection
	dbname := toml.GetConfig().Mysql.DbName     // this is for db name connection
	timeout := "10s"                            // if connection time > 10s, then timeout

	// dsn == Data Source Name
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=UTC&timeout=%s", username, password, host, port, dbname, timeout)
	fmt.Println(dsn)
	var err error

	// Create a new logger
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // Output logs to terminal
		logger.Config{
			SlowThreshold:             time.Second, // SQL queries slower than 1s are considered "slow"
			LogLevel:                  logger.Info, // Log all SQL queries (use logger.Warn or logger.Error if needed)
			IgnoreRecordNotFoundError: true,        // Ignore "record not found" errors
			Colorful:                  true,        // Enable color output in terminal
		},
	)

	_db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		//no config for now but we can add loggers
		Logger: newLogger,
	})

	if err != nil {
		fmt.Println(err)
	}

	sqlDB, _ := _db.DB() // initialise a DB object
	if err := _db.AutoMigrate(
		&entity.MeterReadingsEntity{},
		&entity.JobQueueEntity{},
		&entity.ErrorRowsEntity{},
	); err != nil {
		fmt.Println("Migration failed:", err)
	}

	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetMaxIdleConns(20)

}

func GetDB() *gorm.DB {
	return _db
}

func Save(value interface{}) {
	result := _db.Save(value)
	if result.RowsAffected > 0 {
		// err handling if nothing created
		fmt.Println("No Rows affected")
	}
}
