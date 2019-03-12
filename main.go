package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dbServer   string
	dbPort     int
	dbName     string
	dbUser     string
	dbPassword string
	sqlPath    string
	verPathes  []string
	sqlFiles   []string
)

func init() {
	flag.StringVar(&dbServer, "server", "127.0.0.1", "db server")
	flag.IntVar(&dbPort, "port", 3306, "db port")
	flag.StringVar(&dbName, "name", "", "db name")
	flag.StringVar(&dbUser, "user", "root", "db username")
	flag.StringVar(&dbPassword, "password", "", "db password")
	flag.StringVar(&sqlPath, "sql", "./sql", "sql path")
}

func main() {
	flag.Parse()
	if dbPassword == "" || dbName == "" {
		flag.Usage()
		return
	}

	err := exists(sqlPath)
	if err != nil {
		panic(err)
	}

	if !isDir(sqlPath) {
		panic(errors.New("sqlPath is not a path"))
	}
	dbConnStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&multiStatements=true", dbUser, dbPassword, dbServer, dbPort, dbName)
	db, err := sql.Open("mysql", dbConnStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	version := ""
	err = db.QueryRow("SHOW TABLES LIKE 'dbVer';").Scan(&version)
	if err != nil {
		_, err := db.Exec(`CREATE TABLE dbVer (
			version int(255) NOT NULL,
			addTime datetime(0) NULL,
			PRIMARY KEY (version)
		);
		INSERT INTO dbVer (version,addTime) VALUES (0,NOW());`)
		if err != nil {
			panic(err)
		}
	}

	var maxVersion int
	err = db.QueryRow("SELECT MAX(version) FROM dbVer;").Scan(&maxVersion)
	if err != nil {
		panic(err)
	}
	fmt.Println("The max version in db: ", maxVersion)
	err = filepath.Walk(sqlPath, getAllVerDirs)
	if err != nil {
		panic(err)
	}
	sort.SliceStable(verPathes, func(i, j int) bool {
		return getDirVerNum(verPathes[i]) < getDirVerNum(verPathes[j])
	})
	for _, verDir := range verPathes {
		ver := getDirVerNum(verDir)
		if ver <= maxVersion {
			continue
		}
		sqlFiles = nil
		err = filepath.Walk(verDir, getAllSQLFiles)
		if err != nil {
			panic(err)
		}
		fmt.Println("Exec ver: ", ver)
		err = execSQLFiles(sqlFiles, ver, db)
		if err != nil {
			panic(err)
		}
	}
}

func getDirVerNum(verDir string) int {
	strs := strings.Split(verDir, "/")
	ver := strings.Replace(strs[len(strs)-1], "ver", "", 1)
	result, err := strconv.Atoi(ver)
	if err != nil {
		return -1
	}
	return result
}

func exists(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return nil
		}
		return err
	}
	return nil
}

func isDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func getAllVerDirs(path string, info os.FileInfo, err error) error {
	if info.IsDir() && strings.HasPrefix(info.Name(), "ver") {
		verPathes = append(verPathes, path)
	}
	return nil
}

func getAllSQLFiles(path string, info os.FileInfo, err error) error {
	if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
		sqlFiles = append(sqlFiles, path)
	}
	return nil
}

func execSQLFiles(files []string, ver int, db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for _, file := range files {
		sqlBytes, err := ioutil.ReadFile(file)
		if err != nil {
			tx.Rollback()
			return err
		}
		stmt, err := tx.Prepare(string(sqlBytes))
		if err != nil {
			tx.Rollback()
			return err
		}
		defer stmt.Close()
		if _, err := stmt.Exec(); err != nil {
			tx.Rollback()
			return err
		}
	}
	stmt, err := tx.Prepare("INSERT INTO dbVer (version,addTime) VALUES (?,NOW());")
	if err != nil {
		tx.Rollback()
		return err
	}
	if _, err := stmt.Exec(ver); err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	return tx.Commit()
}
