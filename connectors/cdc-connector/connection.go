package main

import (
	"fmt"
	"log"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB
var dsn = "host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=Asia/Jakarta"

func Connect() error {
	var err error

	DB, err = gorm.Open(postgres.Open(fmt.Sprintf(dsn, os.Getenv("DB_HOST"), os.Getenv("DB_USER"), os.Getenv("DB_PASS"), os.Getenv("DB_NAME"), os.Getenv("DB_PORT"))), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
		return err
	}
	log.Println("success connection database DB")
	return nil
}
