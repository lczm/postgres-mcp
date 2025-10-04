package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/go-faker/faker/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var testDB *embeddedpostgres.EmbeddedPostgres

// Test data structures
type User struct {
	ID        int
	Username  string
	Email     string
	FirstName string
	LastName  string
	Bio       string
	CreatedAt time.Time
}

type Post struct {
	ID        int
	UserID    int
	Content   string
	Title     string
	CreatedAt time.Time
}

type Friendship struct {
	ID         int
	UserID     int
	FriendID   int
	Status     string
	CreatedAt  time.Time
}

type Listing struct {
	ID          int
	UserID      int
	Title       string
	Description string
	Price       float64
	Category    string
	CreatedAt   time.Time
}

type Comment struct {
	ID        int
	PostID    int
	UserID    int
	Content   string
	CreatedAt time.Time
}

func TestMain(m *testing.M) {
	// Start embedded PostgreSQL
	testDB = embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Port(5555).
		Database("testdb").
		Username("testuser").
		Password("testpass"))

	if err := testDB.Start(); err != nil {
		log.Fatalf("Failed to start embedded postgres: %v", err)
	}

	// Connect to database
	connStr := "postgres://testuser:testpass@localhost:5555/testdb?sslmode=disable"
	var err error
	pool, err = pgxpool.New(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Failed to connect to test database: %v", err)
	}

	// Create schema and seed data
	if err := createSchema(); err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}

	if err := seedData(); err != nil {
		log.Fatalf("Failed to seed data: %v", err)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	pool.Close()
	if err := testDB.Stop(); err != nil {
		log.Printf("Failed to stop embedded postgres: %v", err)
	}

	os.Exit(code)
}

func createSchema() error {
	ctx := context.Background()

	schema := `
	-- Users table
	CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		username VARCHAR(50) UNIQUE NOT NULL,
		email VARCHAR(100) UNIQUE NOT NULL,
		first_name VARCHAR(50),
		last_name VARCHAR(50),
		bio TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
	);

	-- Create index on email for faster lookups
	CREATE INDEX idx_users_email ON users(email);
	CREATE INDEX idx_users_created_at ON users(created_at);

	-- Posts table
	CREATE TABLE posts (
		id SERIAL PRIMARY KEY,
		user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		title VARCHAR(200) NOT NULL,
		content TEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT title_not_empty CHECK (LENGTH(title) > 0)
	);

	CREATE INDEX idx_posts_user_id ON posts(user_id);
	CREATE INDEX idx_posts_created_at ON posts(created_at DESC);

	-- Friendships table (self-referencing many-to-many)
	CREATE TABLE friendships (
		id SERIAL PRIMARY KEY,
		user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		friend_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'rejected')),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(user_id, friend_id),
		CONSTRAINT no_self_friendship CHECK (user_id != friend_id)
	);

	CREATE INDEX idx_friendships_user_id ON friendships(user_id);
	CREATE INDEX idx_friendships_friend_id ON friendships(friend_id);
	CREATE INDEX idx_friendships_status ON friendships(status);

	-- Listings table
	CREATE TABLE listings (
		id SERIAL PRIMARY KEY,
		user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		title VARCHAR(200) NOT NULL,
		description TEXT,
		price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
		category VARCHAR(50),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX idx_listings_user_id ON listings(user_id);
	CREATE INDEX idx_listings_category ON listings(category);
	CREATE INDEX idx_listings_price ON listings(price);

	-- Comments table
	CREATE TABLE comments (
		id SERIAL PRIMARY KEY,
		post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
		user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		content TEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT content_not_empty CHECK (LENGTH(content) > 0)
	);

	CREATE INDEX idx_comments_post_id ON comments(post_id);
	CREATE INDEX idx_comments_user_id ON comments(user_id);

	-- Create a view for post statistics
	CREATE VIEW post_stats AS
	SELECT 
		p.id as post_id,
		p.title,
		u.username,
		COUNT(c.id) as comment_count,
		p.created_at
	FROM posts p
	JOIN users u ON p.user_id = u.id
	LEFT JOIN comments c ON p.id = c.post_id
	GROUP BY p.id, p.title, u.username, p.created_at;
	`

	_, err := pool.Exec(ctx, schema)
	return err
}

func seedData() error {
	ctx := context.Background()
	log.Println("Starting optimized data seeding with single transaction...")

	// Start a single transaction for all inserts
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Reduced counts (10% of original)
	const (
		numUsers       = 1000  // was 10,000
		numPosts       = 5000  // was 50,000
		numFriendships = 3000  // was 30,000
		numListings    = 2000  // was 20,000
		numComments    = 10000 // was 100,000
	)

	// Seed users with batch insert
	log.Println("Seeding users...")
	userIDs := make([]int, numUsers)
	batchSize := 500
	
	for batch := 0; batch < numUsers; batch += batchSize {
		end := batch + batchSize
		if end > numUsers {
			end = numUsers
		}
		
		// Build multi-row insert
		valueStrings := make([]string, 0, batchSize)
		valueArgs := make([]interface{}, 0, batchSize*6)
		
		for i := batch; i < end; i++ {
			pos := len(valueArgs)
			valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", 
				pos+1, pos+2, pos+3, pos+4, pos+5, pos+6))
			
			valueArgs = append(valueArgs,
				fmt.Sprintf("user_%d_%s", i, faker.Username()),
				fmt.Sprintf("user%d_%s@example.com", i, faker.Username()),
				faker.FirstName(),
				faker.LastName(),
				faker.Sentence(),
				time.Now().Add(-time.Duration(i)*time.Hour),
			)
		}
		
		query := fmt.Sprintf("INSERT INTO users (username, email, first_name, last_name, bio, created_at) VALUES %s RETURNING id",
			strings.Join(valueStrings, ","))
		
		rows, err := tx.Query(ctx, query, valueArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert users batch: %v", err)
		}
		
		idx := batch
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan user id: %v", err)
			}
			userIDs[idx] = id
			idx++
		}
		rows.Close()
		
		log.Printf("Inserted %d/%d users", end, numUsers)
	}

	// Seed posts with batch insert
	log.Println("Seeding posts...")
	postIDs := make([]int, numPosts)
	
	for batch := 0; batch < numPosts; batch += batchSize {
		end := batch + batchSize
		if end > numPosts {
			end = numPosts
		}
		
		valueStrings := make([]string, 0, batchSize)
		valueArgs := make([]interface{}, 0, batchSize*4)
		
		for i := batch; i < end; i++ {
			pos := len(valueArgs)
			valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d)", 
				pos+1, pos+2, pos+3, pos+4))
			
			userID := userIDs[i%len(userIDs)]
			valueArgs = append(valueArgs,
				userID,
				faker.Sentence(),
				faker.Paragraph(),
				time.Now().Add(-time.Duration(i)*time.Minute),
			)
		}
		
		query := fmt.Sprintf("INSERT INTO posts (user_id, title, content, created_at) VALUES %s RETURNING id",
			strings.Join(valueStrings, ","))
		
		rows, err := tx.Query(ctx, query, valueArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert posts batch: %v", err)
		}
		
		idx := batch
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan post id: %v", err)
			}
			postIDs[idx] = id
			idx++
		}
		rows.Close()
		
		log.Printf("Inserted %d/%d posts", end, numPosts)
	}

	// Seed friendships with batch insert
	log.Println("Seeding friendships...")
	friendshipMap := make(map[string]bool)
	statuses := []string{"pending", "accepted", "rejected"}
	
	for batch := 0; batch < numFriendships; batch += batchSize {
		end := batch + batchSize
		if end > numFriendships {
			end = numFriendships
		}
		
		valueStrings := make([]string, 0, batchSize)
		valueArgs := make([]interface{}, 0, batchSize*4)
		
		for i := batch; i < end; i++ {
			userID := userIDs[i%len(userIDs)]
			friendID := userIDs[(i+1)%len(userIDs)]
			
			// Ensure no duplicates and no self-friendship
			key := fmt.Sprintf("%d-%d", userID, friendID)
			if friendshipMap[key] || userID == friendID {
				continue
			}
			friendshipMap[key] = true
			
			pos := len(valueArgs)
			valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d)", 
				pos+1, pos+2, pos+3, pos+4))
			
			status := statuses[i%3]
			valueArgs = append(valueArgs,
				userID,
				friendID,
				status,
				time.Now().Add(-time.Duration(i)*time.Hour),
			)
		}
		
		if len(valueStrings) > 0 {
			query := fmt.Sprintf("INSERT INTO friendships (user_id, friend_id, status, created_at) VALUES %s",
				strings.Join(valueStrings, ","))
			
			_, err := tx.Exec(ctx, query, valueArgs...)
			if err != nil {
				return fmt.Errorf("failed to insert friendships batch: %v", err)
			}
		}
		
		log.Printf("Inserted %d/%d friendships", end, numFriendships)
	}

	// Seed listings with batch insert
	log.Println("Seeding listings...")
	categories := []string{"Electronics", "Furniture", "Clothing", "Books", "Sports", "Toys", "Home", "Garden"}
	
	for batch := 0; batch < numListings; batch += batchSize {
		end := batch + batchSize
		if end > numListings {
			end = numListings
		}
		
		valueStrings := make([]string, 0, batchSize)
		valueArgs := make([]interface{}, 0, batchSize*6)
		
		for i := batch; i < end; i++ {
			pos := len(valueArgs)
			valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", 
				pos+1, pos+2, pos+3, pos+4, pos+5, pos+6))
			
			userID := userIDs[i%len(userIDs)]
			valueArgs = append(valueArgs,
				userID,
				faker.Sentence(),
				faker.Paragraph(),
				float64(i%1000)+9.99,
				categories[i%len(categories)],
				time.Now().Add(-time.Duration(i)*time.Hour),
			)
		}
		
		query := fmt.Sprintf("INSERT INTO listings (user_id, title, description, price, category, created_at) VALUES %s",
			strings.Join(valueStrings, ","))
		
		_, err := tx.Exec(ctx, query, valueArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert listings batch: %v", err)
		}
		
		log.Printf("Inserted %d/%d listings", end, numListings)
	}

	// Seed comments with batch insert
	log.Println("Seeding comments...")
	
	for batch := 0; batch < numComments; batch += batchSize {
		end := batch + batchSize
		if end > numComments {
			end = numComments
		}
		
		valueStrings := make([]string, 0, batchSize)
		valueArgs := make([]interface{}, 0, batchSize*4)
		
		for i := batch; i < end; i++ {
			pos := len(valueArgs)
			valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d)", 
				pos+1, pos+2, pos+3, pos+4))
			
			postID := postIDs[i%len(postIDs)]
			userID := userIDs[i%len(userIDs)]
			valueArgs = append(valueArgs,
				postID,
				userID,
				faker.Sentence(),
				time.Now().Add(-time.Duration(i)*time.Second),
			)
		}
		
		query := fmt.Sprintf("INSERT INTO comments (post_id, user_id, content, created_at) VALUES %s",
			strings.Join(valueStrings, ","))
		
		_, err := tx.Exec(ctx, query, valueArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert comments batch: %v", err)
		}
		
		log.Printf("Inserted %d/%d comments", end, numComments)
	}

	// Commit the transaction
	log.Println("Committing transaction...")
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	log.Printf("Data seeding completed! Total: %d users, %d posts, %d friendships, %d listings, %d comments",
		numUsers, numPosts, numFriendships, numListings, numComments)
	return nil
}

// Helper function to create mock request with raw arguments (for ExplainAnalyze)
// Most functions don't use the request, so we can just return nil for them
func createMockRequest(args interface{}) *mcp.CallToolRequest {
	return nil
}

func TestListTables(t *testing.T) {
	ctx := context.Background()
	
	t.Run("list tables with public schema", func(t *testing.T) {
		args := TableListArgs{Schema: "public"}
		result, data, err := ListTables(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ListTables failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		tables := data.([]map[string]interface{})
		if len(tables) < 5 {
			t.Errorf("Expected at least 5 tables, got %d", len(tables))
		}
		
		// Verify expected tables exist
		tableNames := make(map[string]bool)
		for _, table := range tables {
			tableNames[table["table_name"].(string)] = true
		}
		
		expectedTables := []string{"users", "posts", "friendships", "listings", "comments"}
		for _, expectedTable := range expectedTables {
			if !tableNames[expectedTable] {
				t.Errorf("Expected table %s not found", expectedTable)
			}
		}
	})
	
	t.Run("list tables with default schema", func(t *testing.T) {
		args := TableListArgs{Schema: ""}
		result, _, err := ListTables(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ListTables failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
	})
}

func TestGetTableSchema(t *testing.T) {
	ctx := context.Background()
	
	t.Run("get schema for users table", func(t *testing.T) {
		args := TableSchemaArgs{TableName: "users", Schema: "public"}
		result, data, err := GetTableSchema(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("GetTableSchema failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		columns := data.([]map[string]interface{})
		if len(columns) != 7 {
			t.Errorf("Expected 7 columns for users table, got %d", len(columns))
		}
		
		// Verify expected columns
		columnNames := make(map[string]bool)
		for _, col := range columns {
			columnNames[col["column_name"].(string)] = true
		}
		
		expectedColumns := []string{"id", "username", "email", "first_name", "last_name", "bio", "created_at"}
		for _, expectedCol := range expectedColumns {
			if !columnNames[expectedCol] {
				t.Errorf("Expected column %s not found", expectedCol)
			}
		}
	})
	
	t.Run("get schema for posts table", func(t *testing.T) {
		args := TableSchemaArgs{TableName: "posts", Schema: ""}
		result, data, err := GetTableSchema(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("GetTableSchema failed: %v", err)
		}
		
		columns := data.([]map[string]interface{})
		if len(columns) != 5 {
			t.Errorf("Expected 5 columns for posts table, got %d", len(columns))
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
	})
}

func TestGetTableConstraints(t *testing.T) {
	ctx := context.Background()
	
	t.Run("get constraints for users table", func(t *testing.T) {
		args := TableConstraintsArgs{TableName: "users", Schema: "public"}
		result, data, err := GetTableConstraints(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("GetTableConstraints failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		constraints := data.([]map[string]interface{})
		if len(constraints) == 0 {
			t.Error("Expected constraints for users table")
		}
		
		// Verify primary key exists
		hasPrimaryKey := false
		hasUniqueEmail := false
		hasCheckConstraint := false
		
		for _, constraint := range constraints {
			constraintType := constraint["constraint_type"].(string)
			if constraintType == "PRIMARY KEY" {
				hasPrimaryKey = true
			}
			if constraintType == "UNIQUE" && constraint["column_name"] == "email" {
				hasUniqueEmail = true
			}
			if constraintType == "CHECK" {
				hasCheckConstraint = true
			}
		}
		
		if !hasPrimaryKey {
			t.Error("Expected primary key constraint")
		}
		if !hasUniqueEmail {
			t.Error("Expected unique constraint on email")
		}
		if !hasCheckConstraint {
			t.Error("Expected check constraint")
		}
	})
	
	t.Run("get constraints for posts table with foreign keys", func(t *testing.T) {
		args := TableConstraintsArgs{TableName: "posts", Schema: "public"}
		result, data, err := GetTableConstraints(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("GetTableConstraints failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		constraints := data.([]map[string]interface{})
		
		// Verify foreign key exists
		hasForeignKey := false
		for _, constraint := range constraints {
			if constraint["constraint_type"].(string) == "FOREIGN KEY" {
				hasForeignKey = true
				if constraint["foreign_table_name"] != "users" {
					t.Errorf("Expected foreign key to users table, got %v", constraint["foreign_table_name"])
				}
			}
		}
		
		if !hasForeignKey {
			t.Error("Expected foreign key constraint")
		}
	})
	
	t.Run("get constraints for friendships with self-referencing", func(t *testing.T) {
		args := TableConstraintsArgs{TableName: "friendships", Schema: "public"}
		result, data, err := GetTableConstraints(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("GetTableConstraints failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		constraints := data.([]map[string]interface{})
		
		// Count foreign keys (should have 2: user_id and friend_id)
		foreignKeyCount := 0
		for _, constraint := range constraints {
			if constraint["constraint_type"].(string) == "FOREIGN KEY" {
				foreignKeyCount++
			}
		}
		
		if foreignKeyCount < 2 {
			t.Errorf("Expected at least 2 foreign keys for friendships table, got %d", foreignKeyCount)
		}
	})
}

func TestGetTableIndexes(t *testing.T) {
	ctx := context.Background()
	
	t.Run("get indexes for users table", func(t *testing.T) {
		args := TableIndexesArgs{TableName: "users", Schema: "public"}
		result, data, err := GetTableIndexes(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("GetTableIndexes failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		indexes := data.([]map[string]interface{})
		if len(indexes) == 0 {
			t.Error("Expected indexes for users table")
		}
		
		// Verify primary key index exists
		hasPrimaryIndex := false
		hasEmailIndex := false
		
		for _, index := range indexes {
			if index["is_primary"].(bool) {
				hasPrimaryIndex = true
			}
			if indexName, ok := index["index_name"].(string); ok && indexName == "idx_users_email" {
				hasEmailIndex = true
			}
		}
		
		if !hasPrimaryIndex {
			t.Error("Expected primary key index")
		}
		if !hasEmailIndex {
			t.Error("Expected email index")
		}
	})
	
	t.Run("get indexes for posts table", func(t *testing.T) {
		args := TableIndexesArgs{TableName: "posts", Schema: ""}
		result, data, err := GetTableIndexes(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("GetTableIndexes failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		indexes := data.([]map[string]interface{})
		if len(indexes) == 0 {
			t.Error("Expected indexes for posts table")
		}
	})
}

func TestExecuteQuery(t *testing.T) {
	ctx := context.Background()
	
	t.Run("simple select query", func(t *testing.T) {
		args := QueryArgs{Query: "SELECT COUNT(*) as user_count FROM users"}
		result, data, err := ExecuteQuery(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExecuteQuery failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		rows := data.([]map[string]interface{})
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
		
		count := rows[0]["user_count"].(int64)
		if count != 1000 {
			t.Errorf("Expected 1000 users, got %d", count)
		}
	})
	
	t.Run("join query", func(t *testing.T) {
		args := QueryArgs{
			Query: `SELECT u.username, COUNT(p.id) as post_count 
					FROM users u 
					LEFT JOIN posts p ON u.id = p.user_id 
					GROUP BY u.username 
					LIMIT 10`,
		}
		result, data, err := ExecuteQuery(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExecuteQuery failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		rows := data.([]map[string]interface{})
		if len(rows) != 10 {
			t.Errorf("Expected 10 rows, got %d", len(rows))
		}
	})
	
	t.Run("aggregate query", func(t *testing.T) {
		args := QueryArgs{
			Query: `SELECT category, COUNT(*) as count, AVG(price) as avg_price 
					FROM listings 
					GROUP BY category 
					ORDER BY count DESC`,
		}
		result, data, err := ExecuteQuery(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExecuteQuery failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		rows := data.([]map[string]interface{})
		if len(rows) == 0 {
			t.Error("Expected results from aggregate query")
		}
	})
	
	t.Run("view query", func(t *testing.T) {
		args := QueryArgs{Query: "SELECT * FROM post_stats LIMIT 5"}
		result, data, err := ExecuteQuery(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExecuteQuery failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		rows := data.([]map[string]interface{})
		if len(rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(rows))
		}
	})
}

func TestExplainAnalyze(t *testing.T) {
	ctx := context.Background()
	
	t.Run("default settings", func(t *testing.T) {
		args := ExplainAnalyzeArgs{
			Query: "SELECT * FROM users WHERE email LIKE '%example.com' LIMIT 10",
		}
		result, data, err := ExplainAnalyze(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExplainAnalyze failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		if data == nil {
			t.Fatal("Expected data, got nil")
		}
	})
	
	t.Run("analyze false", func(t *testing.T) {
		rawArgs := map[string]interface{}{
			"query":   "SELECT * FROM posts WHERE user_id = 1",
			"analyze": false,
		}
		
		args := ExplainAnalyzeArgs{
			Query:   "SELECT * FROM posts WHERE user_id = 1",
			Analyze: false,
		}
		
		result, _, err := ExplainAnalyze(ctx, createMockRequest(rawArgs), args)
		
		if err != nil {
			t.Fatalf("ExplainAnalyze failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
	})
	
	t.Run("verbose and buffers", func(t *testing.T) {
		rawArgs := map[string]interface{}{
			"query":   "SELECT COUNT(*) FROM comments WHERE post_id IN (SELECT id FROM posts LIMIT 100)",
			"verbose": true,
			"buffers": true,
		}
		
		args := ExplainAnalyzeArgs{
			Query:   "SELECT COUNT(*) FROM comments WHERE post_id IN (SELECT id FROM posts LIMIT 100)",
			Verbose: true,
			Buffers: true,
		}
		
		result, _, err := ExplainAnalyze(ctx, createMockRequest(rawArgs), args)
		
		if err != nil {
			t.Fatalf("ExplainAnalyze failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
	})
	
	t.Run("format text", func(t *testing.T) {
		args := ExplainAnalyzeArgs{
			Query:  "SELECT * FROM listings WHERE price > 500 ORDER BY price DESC LIMIT 20",
			Format: "text",
		}
		result, data, err := ExplainAnalyze(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExplainAnalyze failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		textResult, ok := data.(string)
		if !ok {
			t.Error("Expected string result for text format")
		}
		
		if len(textResult) == 0 {
			t.Error("Expected non-empty text result")
		}
	})
	
	t.Run("insert rollback", func(t *testing.T) {
		// Get count before
		var countBefore int64
		row := pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM users")
		row.Scan(&countBefore)
		
		args := ExplainAnalyzeArgs{
			Query: "INSERT INTO users (username, email, first_name, last_name) VALUES ('rollback_test', 'rollback@test.com', 'Test', 'User')",
		}
		result, _, err := ExplainAnalyze(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExplainAnalyze failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		// Get count after
		var countAfter int64
		row = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM users")
		row.Scan(&countAfter)
		
		if countBefore != countAfter {
			t.Errorf("INSERT was not rolled back: before=%d, after=%d", countBefore, countAfter)
		}
	})
	
	t.Run("update rollback", func(t *testing.T) {
		// Get original email
		var originalEmail string
		row := pool.QueryRow(context.Background(), "SELECT email FROM users WHERE id = 1")
		row.Scan(&originalEmail)
		
		args := ExplainAnalyzeArgs{
			Query: "UPDATE users SET email = 'changed@test.com' WHERE id = 1",
		}
		result, _, err := ExplainAnalyze(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExplainAnalyze failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		// Get email after
		var emailAfter string
		row = pool.QueryRow(context.Background(), "SELECT email FROM users WHERE id = 1")
		row.Scan(&emailAfter)
		
		if originalEmail != emailAfter {
			t.Errorf("UPDATE was not rolled back: original=%s, after=%s", originalEmail, emailAfter)
		}
	})
	
	t.Run("delete rollback", func(t *testing.T) {
		// Get count before
		var countBefore int64
		row := pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM listings")
		row.Scan(&countBefore)
		
		args := ExplainAnalyzeArgs{
			Query: "DELETE FROM listings WHERE id = 1",
		}
		result, _, err := ExplainAnalyze(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExplainAnalyze failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		// Get count after
		var countAfter int64
		row = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM listings")
		row.Scan(&countAfter)
		
		if countBefore != countAfter {
			t.Errorf("DELETE was not rolled back: before=%d, after=%d", countBefore, countAfter)
		}
	})
}

func TestComplexQueries(t *testing.T) {
	ctx := context.Background()
	
	t.Run("multi-join query", func(t *testing.T) {
		args := QueryArgs{
			Query: `
				SELECT 
					u.username,
					COUNT(DISTINCT p.id) as post_count,
					COUNT(DISTINCT c.id) as comment_count,
					COUNT(DISTINCT l.id) as listing_count
				FROM users u
				LEFT JOIN posts p ON u.id = p.user_id
				LEFT JOIN comments c ON u.id = c.user_id
				LEFT JOIN listings l ON u.id = l.user_id
				GROUP BY u.username
				HAVING COUNT(DISTINCT p.id) > 0
				ORDER BY post_count DESC
				LIMIT 10
			`,
		}
		result, data, err := ExecuteQuery(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExecuteQuery failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		rows := data.([]map[string]interface{})
		if len(rows) == 0 {
			t.Error("Expected results from multi-join query")
		}
	})
	
	t.Run("subquery with aggregation", func(t *testing.T) {
		args := QueryArgs{
			Query: `
				SELECT 
					username,
					email,
					(SELECT COUNT(*) FROM posts WHERE user_id = u.id) as total_posts,
					(SELECT COUNT(*) FROM friendships WHERE user_id = u.id) as total_friends
				FROM users u
				WHERE id IN (SELECT user_id FROM posts GROUP BY user_id HAVING COUNT(*) > 1)
				LIMIT 5
			`,
		}
		result, data, err := ExecuteQuery(ctx, createMockRequest(args), args)
		
		if err != nil {
			t.Fatalf("ExecuteQuery failed: %v", err)
		}
		
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		
		rows := data.([]map[string]interface{})
		if len(rows) == 0 {
			t.Error("Expected results from subquery")
		}
	})
}

func TestDataIntegrity(t *testing.T) {
	ctx := context.Background()
	
	t.Run("verify row counts", func(t *testing.T) {
		tables := map[string]int{
			"users":       1000,
			"posts":       5000,
			"comments":    10000,
			"listings":    2000,
		}
		
		for table, expectedCount := range tables {
			var count int64
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
			err := pool.QueryRow(ctx, query).Scan(&count)
			
			if err != nil {
				t.Errorf("Failed to count %s: %v", table, err)
				continue
			}
			
			if int(count) != expectedCount {
				t.Errorf("Expected %d rows in %s, got %d", expectedCount, table, count)
			}
		}
	})
	
	t.Run("verify foreign key relationships", func(t *testing.T) {
		// Check that all post user_ids exist in users table
		var invalidPosts int64
		err := pool.QueryRow(ctx, `
			SELECT COUNT(*) 
			FROM posts p 
			WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = p.user_id)
		`).Scan(&invalidPosts)
		
		if err != nil {
			t.Fatalf("Failed to check foreign keys: %v", err)
		}
		
		if invalidPosts > 0 {
			t.Errorf("Found %d posts with invalid user_id", invalidPosts)
		}
		
		// Check comments
		var invalidComments int64
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*) 
			FROM comments c 
			WHERE NOT EXISTS (SELECT 1 FROM posts p WHERE p.id = c.post_id)
		`).Scan(&invalidComments)
		
		if err != nil {
			t.Fatalf("Failed to check comment foreign keys: %v", err)
		}
		
		if invalidComments > 0 {
			t.Errorf("Found %d comments with invalid post_id", invalidComments)
		}
	})
	
	t.Run("verify constraints", func(t *testing.T) {
		// All friendships should have user_id != friend_id
		var invalidFriendships int64
		err := pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM friendships WHERE user_id = friend_id
		`).Scan(&invalidFriendships)
		
		if err != nil {
			t.Fatalf("Failed to check friendships: %v", err)
		}
		
		if invalidFriendships > 0 {
			t.Errorf("Found %d invalid friendships (self-referencing)", invalidFriendships)
		}
		
		// All listings should have price >= 0
		var invalidListings int64
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM listings WHERE price < 0
		`).Scan(&invalidListings)
		
		if err != nil {
			t.Fatalf("Failed to check listings: %v", err)
		}
		
		if invalidListings > 0 {
			t.Errorf("Found %d listings with negative price", invalidListings)
		}
	})
}
