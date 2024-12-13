package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// MenousDB represents the database client
type MenousDB struct {
	URL      string
	Key      string
	Database string
}

// NewMenousDB creates a new MenousDB client
func NewMenousDB(url, key, database string) *MenousDB {
	// Ensure URL ends with a slash
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}

	return &MenousDB{
		URL:      url,
		Key:      key,
		Database: database,
	}
}

// validateDatabase checks if database is set
func (m *MenousDB) validateDatabase() error {
	if m.Database == "" {
		return fmt.Errorf("no database specified")
	}
	return nil
}

// makeRequest handles common HTTP request logic
func (m *MenousDB) makeRequest(method, endpoint string, headers map[string]string, body interface{}) (*http.Response, error) {
	// Prepare URL
	url := m.URL + endpoint

	// Prepare body
	var bodyReader io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewBuffer(jsonBody)
	}

	// Create request
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	// Execute request
	client := &http.Client{}
	return client.Do(req)
}

// ReadDB retrieves database contents
func (m *MenousDB) ReadDB() (map[string]interface{}, error) {
	if err := m.validateDatabase(); err != nil {
		return nil, err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
	}

	resp, err := m.makeRequest("GET", "read-db", headers, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// CreateDB creates a new database
func (m *MenousDB) CreateDB() (string, error) {
	if err := m.validateDatabase(); err != nil {
		return "", err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
	}

	resp, err := m.makeRequest("POST", "create-db", headers, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// DeleteDB deletes the current database
func (m *MenousDB) DeleteDB() (string, error) {
	if err := m.validateDatabase(); err != nil {
		return "", err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
	}

	resp, err := m.makeRequest("DELETE", "del-database", headers, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// CheckDBExists checks if the database exists
func (m *MenousDB) CheckDBExists() (string, error) {
	if err := m.validateDatabase(); err != nil {
		return "", err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
	}

	resp, err := m.makeRequest("GET", "check-db-exists", headers, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// CreateTable creates a new table in the database
func (m *MenousDB) CreateTable(table string, attributes []string) (string, error) {
	if err := m.validateDatabase(); err != nil {
		return "", err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	body := map[string]interface{}{
		"attributes": attributes,
	}

	resp, err := m.makeRequest("POST", "create-table", headers, body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(responseBody), nil
}

// CheckTableExists checks if a table exists in the database
func (m *MenousDB) CheckTableExists(table string) (string, error) {
	if err := m.validateDatabase(); err != nil {
		return "", err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	resp, err := m.makeRequest("GET", "check-table-exists", headers, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// InsertIntoTable inserts values into a table
func (m *MenousDB) InsertIntoTable(table string, values interface{}) (string, error) {
	if err := m.validateDatabase(); err != nil {
		return "", err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	body := map[string]interface{}{
		"values": values,
	}

	resp, err := m.makeRequest("POST", "insert-into-table", headers, body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(responseBody), nil
}

// GetTable retrieves a table's contents
func (m *MenousDB) GetTable(table string) (interface{}, error) {
	if err := m.validateDatabase(); err != nil {
		return nil, err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	resp, err := m.makeRequest("GET", "get-table", headers, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If JSON decoding fails, return raw text
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, readErr
		}
		return string(body), nil
	}

	return result, nil
}

// SelectWhere retrieves records matching conditions
func (m *MenousDB) SelectWhere(table string, conditions map[string]interface{}) (interface{}, error) {
	if err := m.validateDatabase(); err != nil {
		return nil, err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	body := map[string]interface{}{
		"conditions": conditions,
	}

	resp, err := m.makeRequest("GET", "select-where", headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If JSON decoding fails, return raw text
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, readErr
		}
		return string(body), nil
	}

	return result, nil
}

// SelectColumns retrieves specific columns from a table
func (m *MenousDB) SelectColumns(table string, columns []string) (interface{}, error) {
	if err := m.validateDatabase(); err != nil {
		return nil, err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	body := map[string]interface{}{
		"columns": columns,
	}

	resp, err := m.makeRequest("GET", "select-columns", headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If JSON decoding fails, return raw text
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, readErr
		}
		return string(body), nil
	}

	return result, nil
}

// SelectColumnsWhere retrieves specific columns matching conditions
func (m *MenousDB) SelectColumnsWhere(table string, columns []string, conditions map[string]interface{}) (interface{}, error) {
	if err := m.validateDatabase(); err != nil {
		return nil, err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	body := map[string]interface{}{
		"columns":    columns,
		"conditions": conditions,
	}

	resp, err := m.makeRequest("GET", "select-columns-where", headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If JSON decoding fails, return raw text
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, readErr
		}
		return string(body), nil
	}

	return result, nil
}

// DeleteWhere removes records matching conditions
func (m *MenousDB) DeleteWhere(table string, conditions map[string]interface{}) (interface{}, error) {
	if err := m.validateDatabase(); err != nil {
		return nil, err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	body := map[string]interface{}{
		"conditions": conditions,
	}

	resp, err := m.makeRequest("DELETE", "delete-where", headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If JSON decoding fails, return raw text
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, readErr
		}
		return string(body), nil
	}

	return result, nil
}

// DeleteTable removes an entire table
func (m *MenousDB) DeleteTable(table string) (interface{}, error) {
	if err := m.validateDatabase(); err != nil {
		return nil, err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	resp, err := m.makeRequest("DELETE", "delete-table", headers, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If JSON decoding fails, return raw text
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, readErr
		}
		return string(body), nil
	}

	return result, nil
}

// UpdateWhere updates records matching conditions
func (m *MenousDB) UpdateWhere(table string, conditions, values map[string]interface{}) (interface{}, error) {
	if err := m.validateDatabase(); err != nil {
		return nil, err
	}

	headers := map[string]string{
		"key":      m.Key,
		"database": m.Database,
		"table":    table,
	}

	body := map[string]interface{}{
		"conditions": conditions,
		"values":     values,
	}

	resp, err := m.makeRequest("POST", "update-table", headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If JSON decoding fails, return raw text
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, readErr
		}
		return string(body), nil
	}

	return result, nil
}

// GetDatabases retrieves list of databases
func (m *MenousDB) GetDatabases() (interface{}, error) {
	headers := map[string]string{
		"key": m.Key,
	}

	resp, err := m.makeRequest("GET", "get-databases", headers, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If JSON decoding fails, return raw text
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, readErr
		}
		return string(body), nil
	}

	return result, nil
}

// String returns the database name
func (m *MenousDB) String() string {
	return m.Database
}
